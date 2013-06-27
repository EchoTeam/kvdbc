%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, Buckets} = application:get_env(kvdbc, buckets),
    {ok, { {one_for_one, 5, 10}, riakc_child_specs(Buckets)} }.

riakc_child_specs(Buckets) ->
    lists:map(fun(B) ->
        ProcessName = list_to_atom("riakc_" ++ atom_to_list(B)),
        {ProcessName, { riakc_cluster, start_link, [ProcessName] },
            permanent, 10000, worker, [riakc_cluster]}
    end, Buckets).




%% ===================================================================
%% Tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

riakc_child_specs_test_() ->
    [
        fun() ->
            Result = riakc_child_specs([bucket1, bucket2]),
            Expected = [
                {riakc_bucket1, {riakc_cluster, start_link,
                    [riakc_bucket1]}, permanent,
                    10000, worker, [riakc_cluster]},
                {riakc_bucket2, {riakc_cluster, start_link,
                    [riakc_bucket2]}, permanent,
                    10000, worker, [riakc_cluster]}
            ],
            ?assertEqual(Expected, Result)
        end
    ].

-endif.
