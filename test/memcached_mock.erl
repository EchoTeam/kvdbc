-module(memcached_mock).
-export([
	do/3,
	do/4
]).

do(_, get, Key) ->
	case get(Key) of
		undefined -> {error, notfound};
		V -> {ok, V}
	end;
do(_, delete, Key) -> erase(Key).
do(_, {set, _, _}, Key, Val) -> put(Key, Val).
