-module(kvdbc_backend).
-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
		{start_link, 1},
		{get, 3},
		{put, 4},
		{delete, 3},
		{list_buckets, 1},
		{list_keys, 2}
	];
behaviour_info(_) ->
    undefined.
