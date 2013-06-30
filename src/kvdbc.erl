%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:


-module(kvdbc).

-export([
        get/2,
        get/3,
        put/3,
        put/4,
        delete/2,
        delete/3,
        list_keys/1,
        list_keys/2,
        list_keys/3
    ]).

-define(DEFAULT_CLUSTER, default).
-define(CACHE_KEY_EXPIRATION, 5 * 60). % in seconds

% TODO: improve specs
-spec put(ClusterName :: atom(),Table :: binary(),Key::term(),Value::term()) ->  ok | {ok, term()} | {error, term()}.

put(Table, Key, Value) ->
    put(?DEFAULT_CLUSTER, Table, Key, Value).
put(ClusterName, Table, Key, Value) ->
    RClusterName = riakc_cluster_name(ClusterName),
    count(RClusterName, put),
    PutResponse =  riakc_cluster:put(RClusterName, Table, Key, Value, [{w, 2}]),
    case PutResponse of
        ok       -> set_cached_value(mcd_key(RClusterName,Table,Key), Value);
        {ok,_}   -> set_cached_value(mcd_key(RClusterName,Table,Key), Value);
        _ -> no_dice
    end,
    PutResponse.  %% We return result of riak put  operation in any case



-spec get(ClusterName :: atom(), Table :: binary(),Key::term()) -> {ok,term()} | {error,term()}.
get(Table, Key) ->
    get(?DEFAULT_CLUSTER, Table, Key).
get(ClusterName, Table, Key) -> 
    RClusterName = riakc_cluster_name(ClusterName),
    MCDKey = mcd_key(RClusterName, Table, Key),
    case get_cached_value(MCDKey) of
        {ok, CachedValue} ->
            count(RClusterName, get_cached),
            {ok,CachedValue};  %% returns cached value
        _ ->  
            count(RClusterName, get),
            GetResponse =  riakc_cluster:get(RClusterName, Table, Key, [{r, 2}]),
            case GetResponse of
                {ok, Value} -> set_cached_value(MCDKey, Value);
                _   -> no_dice
            end,
            GetResponse  %% returns result of riak get operation 
    end.

-spec delete(ClusterName :: atom(),Table ::binary(),Key::term()) -> ok | {error, term()}.
delete(Table, Key) ->
    delete(?DEFAULT_CLUSTER, Table, Key).
delete(ClusterName, Table, Key) ->
    RClusterName = riakc_cluster_name(ClusterName),
    count(RClusterName, delete),
    DelResponse = riakc_cluster:delete(RClusterName,Table, Key, [{rw, 2}]),
    case DelResponse  of
        ok -> delete_cached_value(mcd_key(RClusterName, Table, Key));
        _ ->  no_dice
    end,
    DelResponse.  %% We return result of riak del opration in any case

list_keys(Table) -> list_keys(?DEFAULT_CLUSTER, Table).
list_keys(ClusterName, Table) ->
    RClusterName = riakc_cluster_name(ClusterName),
    riakc_cluster:list_keys(RClusterName, Table).
list_keys(ClusterName, Table, Timeout) ->
    RClusterName = riakc_cluster_name(ClusterName),
    riakc_cluster:list_keys(RClusterName, Table, Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

external_module_call(ModuleKey, Fun, Args) ->
    case application:get_env(kvdbc, ModuleKey) of
        {ok, Module} -> erlang:apply(Module, Fun, Args);
        _ -> ignore
    end.

mcd_key(ProcName, Table, Key) ->
    {?MODULE, ProcName, Table, Key}.

get_cached_value(MCDKey) ->
    external_module_call(memcached_module, do, [mb_riak_cache, get, MCDKey]).

set_cached_value(MCDKey, Value) ->
    external_module_call(memcached_module, do,
        [mb_riak_cache, {set, 0, ?CACHE_KEY_EXPIRATION}, MCDKey, Value]).

delete_cached_value(MCDKey) ->
    external_module_call(memcached_module, do, [mb_riak_cache, delete, MCDKey]).

count(RClusterName, Op) ->
    CounterName = ["riakc.", atom_to_list(RClusterName), ".",
        atom_to_list(Op), ".r.all"],
    external_module_call(metrics_module, notify,
        [iolist_to_binary(CounterName), {inc, 1}]),
    ok.

riakc_cluster_name(ClusterName) ->
    list_to_atom("riakc_" ++ atom_to_list(ClusterName)).
