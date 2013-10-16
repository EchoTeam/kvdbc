%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_riak_backend_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

kvdbc_handler_call_test_() ->
    {setup,
        fun kvdbc_handler_call_setup/0,
        fun(_) -> meck:unload() end,
        [{"exception", fun test_kvdbc_handler_call_exception/0},
        {"error", fun test_kvdbc_handler_call_error/0}]
    }.

kvdbc_handler_call_setup() ->
    meck:new(riakc_cluster),
    meck:expect(riakc_cluster, get, fun(_ClusterName, _Table, Key, _Options) ->
        case Key of
            <<"throw">> ->
                erlang:throw(somethrow);
            <<"error">> ->
                {error, someerror}
        end
    end).

test_kvdbc_handler_call_exception() ->
    ?assertEqual({throw, somethrow}, try kvdbc_riak_backend:get(test, test, <<"table">>, <<"throw">>) catch C:R -> {C, R} end).

test_kvdbc_handler_call_error() ->
    ?assertEqual({error, {riak, someerror}}, kvdbc_riak_backend:get(test, test, <<"table">>, <<"error">>)).

-endif.
