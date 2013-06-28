%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:


-module(kvdbc).

-export([
        get/2,
        get/3,
        get/4,
        put/3,
        put/4,
        put/5,
        delete/2,
        delete/3,
        delete/4,
        list_keys/1,
        list_keys/2,
        list_keys/3
    ]).

-define(DEFAULT_CLUSTER, riakc_default).
-define(CACHE_KEY_EXPIRATION, 5 * 60). % in seconds

-spec put(ClusterName :: atom(),Table :: binary(),Key::term(),Value::term(),Options::[proplists:property()]) ->  ok | {ok, term()} | {error, term()}.

put(Table, Key, Value) ->
    put(?DEFAULT_CLUSTER, Table, Key, Value, []).
put(Table, Key, Value, Options) when is_binary(Table) ->
    put(?DEFAULT_CLUSTER, Table, Key, Value, Options);
put(ClusterName, Table, Key, Value) ->
    put(ClusterName, Table, Key, Value, []).
put(ClusterName, Table, Key, Value, Options) ->
    count(ClusterName, put),
    PutResponse =  riakc_cluster:put(ClusterName, Table,Key,Value,Options),
    case PutResponse of
        ok       -> set_cached_value(mcd_key(ClusterName,Table,Key), Value);
        {ok,_}   -> set_cached_value(mcd_key(ClusterName,Table,Key), Value);
        _ -> no_dice
    end,
    PutResponse.  %% We return result of riak put  operation in any case



-spec get(ClusterName :: atom(), Table :: binary(),Key::term(),Options::[proplists:property()]) -> {ok,term()} | {error,term()}.
get(Table, Key) ->
    get(?DEFAULT_CLUSTER, Table, Key, []).
get(Table, Key, Options) when is_binary(Table) ->
    get(?DEFAULT_CLUSTER, Table, Key, Options);
get(ClusterName, Table, Key) ->
    get(ClusterName, Table, Key, []).
get(ClusterName, Table, Key, Options) -> 
    MCDKey = mcd_key(ClusterName, Table, Key),
    case get_cached_value(MCDKey) of
        {ok, CachedValue} ->
            count(ClusterName, get_cached),
            {ok,CachedValue};  %% returns cached value
        _ ->  
            count(ClusterName, get),
            GetResponse =  riakc_cluster:get(ClusterName,Table,Key,Options),
            case GetResponse of
                {ok, Value} -> set_cached_value(MCDKey, Value);
                _   -> no_dice
            end,
            GetResponse  %% returns result of riak get operation 
    end.

-spec delete(ClusterName :: atom(),Table ::binary(),Key::term(),Options::[proplists:property()]) -> ok | {error, term()}.
delete(Table, Key) ->
    delete(?DEFAULT_CLUSTER, Table, Key, []).
delete(Table, Key, Options) when is_binary(Table) ->
    delete(?DEFAULT_CLUSTER, Table, Key, Options);
delete(ClusterName, Table, Key) ->
    delete(ClusterName, Table, Key, []).
delete(ClusterName, Table, Key, Options) ->
    count(ClusterName, delete),
    DelResponse = riakc_cluster:delete(ClusterName,Table, Key, Options),
    case DelResponse  of
        ok -> delete_cached_value(mcd_key(ClusterName, Table, Key));
        _ ->  no_dice
    end,
    DelResponse.  %% We return result of riak del opration in any case

list_keys(Table) -> list_keys(?DEFAULT_CLUSTER, Table).
list_keys(ClusterName, Table) -> riakc_cluster:list_keys(ClusterName,Table).
list_keys(ClusterName, Table, Timeout) -> riakc_cluster:list_keys(ClusterName, Table, Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

external_module(ModuleKey, Fun, Args) ->
    case application:get_env(kvdbc, ModuleKey) of
        {ok, Module} -> Module;
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

count(ClusterName, Op) ->
    CounterName = ["riakc.", atom_to_list(ClusterName), ".",
        atom_to_list(Op), ".r.all"],
    external_module_call(metrics_module, notify,
        [iolist_to_binary(CounterName), {inc, 1}]),
    ok.
