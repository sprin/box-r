import datetime
import gevent

# Monkey-patch.
gevent.monkey.patch_all(thread=False, select=False)

def get_document_id(path, file_map):
    path_tuple = tuple(path.split('/'))
    return file_map[path_tuple]

def build_file_maps(folder_dict, path_list=[]):
    """
    For every file, build a map where the keys are the path to the file
    and the values are the id.
    """
    folder_map = {}
    name = folder_dict.get('name')
    # Append the current directory name to the list of paths
    path_list = path_list + [name]

    # Documents may only be placed in a leaf directory.
    folders = folder_dict.get('folders')
    if folders:
        for f in folders:
            folder_map.update(build_file_maps(f, path_list))
    else:
        files = folder_dict.get('files')
        for file in files:
            folder_map[tuple(path_list)] = file['id']
    return folder_map

def get_subfolders(id, box_client):
    """
    Recursively walk sub-directories and return folder ids/names and file
    id/names in a nested dictionary.
    """
    print '[{0}] GET folders/{1}/items'.format(datetime.datetime.now(), id)
    folder_json = box_client.get_folder_content(id)
    jobs = []
    if folder_json['item_collection']:

        folder_ids = [x['id']
            for x in folder_json['item_collection']['entries']
            if x['type'] == 'folder']

        files = [{'id': x['id'], 'name': x['name']}
            for x in folder_json['item_collection']['entries']
            if x['type'] == 'file']
        # Spawn greenlets to get subfolders
        # Note that grequests is *not* used because we do not want to wait
        # for all requests fo finish before recursing deeper.
        jobs = [gevent.spawn(get_subfolders, f_id, box_client)
                for f_id in folder_ids]
    # Wait until greenlets finish before returning
    gevent.joinall(jobs)
    folder_list = [job.value for job in jobs]
    return {
        'folders': folder_list,
        'files': files,
        'name': folder_json['name']
    }

