-module(lager_imem).
-behaviour(gen_event).
-include_lib("imem/include/imem_if.hrl").

%% gen_event callbacks
-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([trace/1, trace/2]).
-export([test/0]).

-record(state, {
        level=info,
        tables=[],
        user,
        db,
        password,
        default_table=?MODULE,
        default_record=?MODULE,
        session}).


%%%===================================================================
%%% trace
%%%===================================================================
trace(Filter) ->
    trace(Filter, debug).
trace(Filter, Level) ->
    Trace0 = {Filter, Level, ?MODULE},
    case lager_util:validate_trace(Trace0) of
        {ok, Trace} ->
            {MinLevel, Traces} = lager_config:get(loglevel),
            case lists:member(Trace, Traces) of
                false ->
                    lager_config:set(loglevel, {MinLevel, [Trace|Traces]});
                _ ->
                    ok
            end,
            {ok, Trace};
        Error ->
            Error
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================
setup_table(ImemSession, Name, []) ->
    setup_table(ImemSession, Name, record_info(fields, ddLog), ?ddLog, #ddLog{});
setup_table(ImemSession, Name, Configuration) ->
    LogFieldDefs = [
            {logTime, timestamp},
            {logLevel, atom},
            {pid, pid},
            {module, atom},
            {function, atom},
            {line, integer},
            {node, atom},
            {fields, list},
            {message, binary},
            {stacktrace, list}
            ],

    FieldDefs = LogFieldDefs ++ proplists:get_value(fields, Configuration, []), % KV-pairs of field/type definitions
    {Fields, Types} = lists:unzip(FieldDefs),
    DefFun = fun
        (integer) -> 0;
        (list) -> [];
        (binstr) -> <<"">>;
        (binary) -> <<>>;
        (_) -> undefined
    end,
    Defaults = list_to_tuple([Name|[DefFun(T) || T <- Types]]),
    setup_table(ImemSession, Name, Fields, Types, Defaults).

setup_table(ImemSession, Name, Fields, Types, Defaults) ->
    RecordName = element(1, Defaults),
    ImemSession:run_cmd(create_table, [Name, {Fields, Types, Defaults}, [{record_name, RecordName}, {type, ordered_set}], lager_imem]),
    ImemSession:run_cmd(check_table, [Name]).

init(Params) ->
    application:start(imem),
    State = state_from_params(#state{}, Params),
    Password = erlang:md5(State#state.password),
    Cred = {State#state.user, Password},
    ImemSession = erlimem:open(local, {State#state.db}, Cred),
    [setup_table(ImemSession, Name, Configuration) || {Name, Configuration} <- State#state.tables ++ [{State#state.default_table, []}]],
    {ok, State#state{session=ImemSession}}.

handle_event({log, LagerMsg}, State = #state{tables=Tables, default_table=DefaultTable, default_record=DefaultRecord, session=ImemSession, level = LogLevel}) ->
    case lager_util:is_loggable(LagerMsg, LogLevel, ?MODULE) of
        true ->
            Level = lager_msg:severity_as_int(LagerMsg),
            %{Date, Time} = lager_msg:timestamp(LagerMsg),
            Date = erlang:now(),
            Message = lager_msg:message(LagerMsg),
            Metadata = lager_msg:metadata(LagerMsg),
            Mod = proplists:get_value(module, Metadata),
            Fun = proplists:get_value(function, Metadata),
            Line = proplists:get_value(line, Metadata),

            Pid = proplists:get_value(pid, Metadata),
            Fields = [P || {K,_} = P <- Metadata, K /= node , K /= application, K /= module, K /= function, K /= line, K /= pid, K /= imem_table],

            LogTable = proplists:get_value(imem_table, Metadata, DefaultTable),
            LogRecord =
            case LogTable == DefaultTable of
                true ->
                    DefaultRecord;
                false ->
                    %% LogTable == LogRecord
                    LogTable
            end,

            Node = node(),
            Configuration = proplists:get_value(LogRecord, Tables, []),

            {FieldData, Fields1} =
            case proplists:get_value(fields, Configuration) of
                undefined ->
                    {[], Fields};
                L ->
                    {[proplists:get_value(Field, Fields) || {Field, _} <- L], []}
            end,
            Entry =
                     lists:append([
                        [
                            LogRecord,
                            Date,
                            lager_util:num_to_level(Level),
                            list_to_pid(Pid),
                            Mod,
                            Fun,
                            Line,
                            Node,
                            Fields1
                        ],
                        [
                            list_to_binary(Message)
                        ],
                        [
                            [] %  Stacktrace
                        ],
                        FieldData
                    ]),

            EntryTuple = list_to_tuple(Entry),
            ImemSession:run_cmd(dirty_write, [LogTable, EntryTuple]);
        false ->
            ok
    end,
    {ok, State};
handle_event({lager_imem_options, Params}, State) ->
    {ok, state_from_params(State, Params)};

handle_event(_Event, State) ->
    {ok, State}.

handle_call({set_loglevel, Level}, State) ->
    {ok, ok, State#state{level = lager_util:level_to_num(Level) }};

handle_call(get_loglevel, State = #state{level = Level}) ->
    {ok, Level, State}.

handle_info(_Info, State) ->
    %% we'll get (unused) log rotate messages
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
state_from_params(OrigState = #state{user = OldUser,
                                     password = OldPassword,
                                     db = OldDb,
                                     level = OldLevel,
                                     default_table = OldDefaultTableName,
                                     default_record = OldDefaultRecord,
                                     tables = OldTables}, Params) ->
    User = proplists:get_value(user, Params, OldUser),
    Password = proplists:get_value(password, Params, OldPassword),
    Db = proplists:get_value(db, Params, OldDb),
    Tables = proplists:get_value(tables, Params, OldTables),
    Level = proplists:get_value(level, Params, OldLevel),
    DefaultTableName = proplists:get_value(default_table, Params, OldDefaultTableName),
    DefaultRecord = proplists:get_value(default_record, Params, OldDefaultRecord),

    OrigState#state{level=lager_util:level_to_num(Level),
                    user=User,
                    password=Password,
                    db=Db,
                    default_table=DefaultTableName,
                    default_record=DefaultRecord,
                    tables=Tables}.

%%%===================================================================
%%% Tests
%%%===================================================================

test() ->
    application:load(lager),
    application:set_env(lager, handlers, [{lager_console_backend, debug},
                                          {lager_imem, [{db, "MproLog"},
                                                        {level, info},
                                                        {user, <<"admin">>},
                                                        {password, <<"change_on_install">>},
                                                        {default_table, 'ddLog@'},
                                                        {default_record, ddLog},
                                                        {tables,[{customers, [
                                                                {fields, [
                                                                        {key, integer},
                                                                        {client_id, list}
                                                                    ]}
                                                                ]}]
                                                            }
                                                        ]},
                                          {lager_file_backend,
                                           [{"error.log", error, 10485760, "$D0", 5},
                                            {"console.log", info, 10485760, "$D0", 5}]}]),
    application:set_env(lager, error_logger_redirect, false),
    lager:start(),
    lager:info("Test INFO message"),
    lager:debug("Test DEBUG message"),
    lager:error("Test ERROR message"),
    lager:info([{imem_table, customers}, {key, 123456}, {client_id, "abc"}], "TEST debug message"),
    lager:warning([{a,b}, {c,d}], "Hello", []),
    lager:info("Info ~p", ["variable"]).
