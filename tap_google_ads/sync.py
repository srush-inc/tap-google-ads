import json
import itertools
import singer
from tap_google_ads.client import create_sdk_client
from tap_google_ads.streams import initialize_core_streams, initialize_reports, initialize_custom_reports

LOGGER = singer.get_logger()
DEFAULT_QUERY_LIMIT = 1000000


def get_currently_syncing(state):
    currently_syncing = state.get("currently_syncing")

    if not currently_syncing:
        currently_syncing = (None, None)

    resuming_stream, resuming_customer = currently_syncing
    return resuming_stream, resuming_customer


def sort_customers(customers):
    return sorted(customers, key=lambda x: x["customerId"])

def sort_selected_streams(sort_list):
    return sorted(sort_list, key=lambda x: x["tap_stream_id"])


def shuffle(shuffle_list, shuffle_key, current_value, sort_function):
    """Return `shuffle_list` with `current_value` at the front of the list

    In the scenario where `current_value` is not in `shuffle_list`:
    - Assume that we have a consistent ordering to `shuffle_list`
    - Insert the `current_value` into `shuffle_list`
    - Sort the new list
    - Do the normal logic to shuffle the list
    - Return the new shuffled list without the `current_value` we inserted"""

    fallback = False
    if current_value not in [item[shuffle_key] for item in shuffle_list]:
        fallback = True
        shuffle_list.append({shuffle_key: current_value})
        shuffle_list = sort_function(shuffle_list)

    matching_index = 0
    for i, key in enumerate(shuffle_list):
        if key[shuffle_key] == current_value:
            matching_index = i
            break
    top_half = shuffle_list[matching_index:]
    bottom_half = shuffle_list[:matching_index]

    if fallback:
        return top_half[1:] + bottom_half

    return top_half + bottom_half

def get_query_limit(config):
    """
    This function will get the query_limit from config,
    and will return the default value if an invalid query limit is given.
    """
    query_limit = config.get('query_limit', DEFAULT_QUERY_LIMIT)

    try:
        if int(float(query_limit)) > 0:
            return int(float(query_limit))
        else:
            LOGGER.warning(f"The entered query limit is invalid; it will be set to the default query limit of {DEFAULT_QUERY_LIMIT}")
            return DEFAULT_QUERY_LIMIT
    except Exception:
        LOGGER.warning(f"The entered query limit is invalid; it will be set to the default query limit of {DEFAULT_QUERY_LIMIT}")
        return DEFAULT_QUERY_LIMIT

def do_sync(config, catalog, resource_schema, state):
    # QA ADDED WORKAROUND [START]
    try:
        customers = json.loads(config["login_customer_ids"])
    except TypeError:  # falling back to raw value
        customers = config["login_customer_ids"]

    # Get query limit
    query_limit = get_query_limit(config)
    # QA ADDED WORKAROUND [END]
    customers = sort_customers(customers)

    selected_streams = [
        stream
        for stream in catalog["streams"]
        if singer.metadata.to_map(stream["metadata"])[()].get("selected")
    ]
    selected_streams = sort_selected_streams(selected_streams)

    core_streams = initialize_core_streams(resource_schema)
    report_streams = initialize_reports(resource_schema)
    custom_report_streams = initialize_custom_reports(resource_schema, config)

    resuming_stream, resuming_customer = get_currently_syncing(state)

    if resuming_stream:
        selected_streams = shuffle(
            selected_streams,
            "tap_stream_id",
            resuming_stream,
            sort_function=sort_selected_streams
        )

    if resuming_customer:
        customers = shuffle(
            customers,
            "customerId",
            resuming_customer,
            sort_function=sort_customers
        )

    for catalog_entry in selected_streams:
        stream_name = catalog_entry["stream"]
        mdata_map = singer.metadata.to_map(catalog_entry["metadata"])

        primary_key = mdata_map[()].get("table-key-properties", [])
        flatten_entry = {"properties": flatten_schema(catalog_entry["schema"], max_level=10)}
        singer.messages.write_schema(stream_name, flatten_entry, primary_key)
        for customer in customers:
            sdk_client = create_sdk_client(config, customer["loginCustomerId"])

            LOGGER.info(f"Syncing {stream_name} for customer Id {customer['customerId']}.")

            if core_streams.get(stream_name):
                stream_obj = core_streams[stream_name]
            elif report_streams.get(stream_name):
                stream_obj = report_streams[stream_name]
            else:
                stream_obj = custom_report_streams[stream_name]

            stream_obj.sync(sdk_client, customer, catalog_entry, config, state, query_limit=query_limit)

    state.pop("currently_syncing", None)
    singer.write_state(state)

def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    return sep.join(full_key)

def flatten_schema(d, parent_key=[], sep='_', level=0, max_level=0):
    items = []

    if 'properties' not in d:
        return {}

    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)
        if 'type' in v.keys():
            if 'object' in v['type'] and 'properties' in v and level < max_level:
                items.extend(flatten_schema(v, parent_key + [k], sep=sep, level=level+1, max_level=max_level).items())
            else:
                items.append((new_key, v))
        else:
            if len(v.values()) > 0:
                if list(v.values())[0][0]['type'] == 'string':
                    list(v.values())[0][0]['type'] = ['null', 'string']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'array':
                    list(v.values())[0][0]['type'] = ['null', 'array']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'object':
                    list(v.values())[0][0]['type'] = ['null', 'object']
                    items.append((new_key, list(v.values())[0][0]))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError('Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)
