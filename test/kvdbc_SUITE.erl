%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).

-export([
    start_app_test/1,
    backend_test/1
]).
 
all() -> [
    start_app_test,
    backend_test
].
 
start_app_test(_Config) ->
    ok = application:start(kvdbc),
    true = whereis(kvdbc_sup) =/= undefined,
    true = whereis(riakc_default) =/= undefined,
    ok = application:stop(kvdbc).

backend_test(_Config) ->
    ok = application:start(kvdbc),
    Key = list_to_binary(test_utils:unique_string()),
    Value = list_to_binary(test_utils:unique_string()),
    ok = kvdbc:put(<<"test_bucket">>, Key, Value),
    {ok, Value} = kvdbc:get(<<"test_bucket">>, Key),
    {ok, Value} = kvdbc:get(default, <<"test_bucket">>, Key),
    ok = kvdbc:delete(<<"test_bucket">>, Key),
    {error, notfound} = kvdbc:get(<<"test_bucket">>, Key),
    ok = application:stop(kvdbc).
