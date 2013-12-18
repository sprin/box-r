import json
import box
import gevent

def get_subfolders(id, box_client):
    """
    Recursively walk sub-directories and return folder ids/names and file
    id/names in a nested dictionary.
    """

    def get_subfolders_inner(id):
        folder_json = box_client.get_folder(id)
        jobs = []
        if folder_json['item_collection']:

            folder_ids = [x['id']
                for x in folder_json['item_collection']['entries']
                if x['type'] == 'folder']

            files = [{'id': x['id'], 'name': x['name']}
                for x in folder_json['item_collection']['entries']
                if x['type'] == 'file']
            # Spawn greenlets to get subfolders
            jobs = [gevent.spawn(get_subfolders_inner, f_id)
                    for f_id in folder_ids]
        # Wait until greenlets finish before returning
        gevent.joinall(jobs)
        folder_list = [job.value for job in jobs]
        return {
            'folders': folder_list,
            'files': files,
            'name': folder_json['name'],
            'id': folder_json['id']
        }

    return get_subfolders_inner(id)

def bulk_create_folders(folder_path_list, box_client, parent_id=0):
    # Keep a map of created/discovered folders to save HTTP requests.
    # This structure is shared and updated by all the greenlets.
    folder_map = {}

    # Set the empty tuple (base path) to the `parent_id` argument.
    folder_map[tuple()] = parent_id

    # Spawn a gevent for every path to be created.
    # These greenlets will switch when they make an HTTP request,
    # or while waiting for a pending folder creation to finish in another
    # greenlet.
    jobs = [
        gevent.spawn(
            _create_nested_folder,
            folder_path,
            box_client,
            folder_map,
        )
        for folder_path in folder_path_list
    ]
    gevent.joinall(jobs)
    return folder_map


def _create_nested_folder(folder_path, box_client, folder_map={}):
    """
    Given the desired full path to a folder, create the folder and
    and parent folders needed, similar to mkdir -p.

    Updates the folder map with any folders created or discovered that
    were not already in the folder map. This is used to keep a cache of
    known folders for bulk folder creation. Updates are visible to other
    greenlets.
    """
    folder_path_parts = folder_path.split('/')

    def create_nested_folder_inner(path_parts):
        # Create, or lookup, the parent id.
        # We may have an empty list for path_parts, denoting the root
        # folder. In which case, we should expect to find a
        # key for an empty tuple in the folder map.
        if path_parts:
            parent_path_parts = path_parts[:-1]
            parent_of_this = create_nested_folder_inner(parent_path_parts)

        # Look up path to see if it is already in the map.
        this_id = folder_map.get(tuple(path_parts))

        # If pending, wait 0.1 second if the folder is pending creation
        while this_id == 'pending':
            gevent.sleep(seconds=0.1)
            this_id = folder_map.get(tuple(path_parts))

        # If the path was already in the map or was created
        # after being pending, return the folder id.
        if this_id is not None:
            return this_id

        folder_name = path_parts[-1]

        # Try to create the folder, and catch the case where it is already
        # exists (but not in our local map).
        try:
            folder_map[tuple(path_parts)] = 'pending'
            print 'Creating {0}'.format('/'.join(path_parts))
            new_folder = box_client.create_folder(folder_name, parent_of_this)
            folder_id = int(new_folder['id'])

        except box.ItemAlreadyExists as e:
            # The id of the existing folder is in the response text,
            # which is in the message of the exception as a json string.
            resp = json.loads(e.message)
            folder_id = int(resp['context_info']['conflicts'][0]['id'])

        # Map the path tuple to the id
        folder_map[tuple(path_parts)] = folder_id
        return folder_id

    create_nested_folder_inner(folder_path_parts)

