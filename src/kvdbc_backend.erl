-module(kvdbc_backend).
-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
        {start_link, 2},
        {get, 4},
        {put, 5},
        {delete, 4},
        {list_buckets, 2},
        {list_keys, 3}
    ];
behaviour_info(_) ->
    undefined.
