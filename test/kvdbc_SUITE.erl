%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).

-export([
    start_app_test/1
]).
 
all() -> [
    start_app_test
].
 
start_app_test(_Config) ->
    ok = application:start(kvdbc),
    true = whereis(kvdbc_sup) =/= undefined,
    true = whereis(riakc_default) =/= undefined,
    ok = application:stop(kvdbc).
