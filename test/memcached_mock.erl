-module(memcached_mock).
-export([
	do/3,
	do/4
]).

do(_, get, _Key) -> undefined;
do(_, delete, _Key) -> ok.
do(_, {set, _, _}, _Key, _Val) -> ok.
