%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(kvdbc_local_backend_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    {setup,
        fun() ->
            {ok, _} = call(start_link, [])
        end,
        fun(_) -> 
            ok = call(stop, [])
        end,
        [
            {"put", fun() ->
                ?assertEqual(ok, call(put, [table1, key1, value1])),
                ?assertEqual({ok, value1}, call(get, [table1, key1]))
            end},
            {"delete", fun() ->
                ?assertEqual(ok, call(put, [table1, key1, value1])),
                ?assertEqual({ok, value1}, call(get, [table1, key1])),
                ?assertEqual(ok, call(delete, [table1, key1])),
                ?assertEqual({ok, undefined}, call(get, [table1, key1]))
            end},
            {"list keys", fun() ->
                ?assertEqual(ok, call(put, [table1, key1, value1])),
                ?assertEqual(ok, call(put, [table1, key2, value2])),
                ?assertEqual({ok, [key1, key2]}, call(list_keys, [table1]))
            end},
            {"list buckets", fun() ->
                ?assertEqual(ok, call(put, [table1, key1, value1])),
                ?assertEqual(ok, call(put, [table2, key2, value2])),
                ?assertEqual({ok, [table1, table2]}, call(list_buckets, []))
            end}
        ]
    }.
    
% private functions

call(Fun, Params) ->
    erlang:apply(kvdbc_local_backend, Fun, [instance, process_name | Params]).

-endif.
