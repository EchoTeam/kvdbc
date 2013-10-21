%%% vim: set ts=4 sts=4 sw=4 et:
-module(memcached_mock).
-export([
    get/2,
    delete/2,
    set/4
]).

get(_, Key) ->
    case get(Key) of
        undefined -> {error, notfound};
        V -> {ok, V}
    end.
delete(_, Key) -> erase(Key).
set(_, Key, Val, _) -> put(Key, Val).
