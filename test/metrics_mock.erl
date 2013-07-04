-module(metrics_mock).
-export([safely_notify/2, get_metric/1]).

safely_notify(CounterName, {inc, 1}) ->
	V = get_metric(CounterName),
	put(CounterName, V + 1).

get_metric(CounterName) ->
	case get(CounterName) of
		undefined -> 0;
		V -> V
	end.
