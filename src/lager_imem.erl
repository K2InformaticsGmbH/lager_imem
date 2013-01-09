-module(lager_imem).
-behaviour(gen_event).

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
        password,
        db,
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
setup_table(ImemSession, Name, Configuration) ->
    LogFieldDefs = [
            {datetime, list},
            {level, list},
            {pid, list},
            {module, list},
            {function, list},
            {line, integer},
            {node, list}
            ],

    FieldDefs = LogFieldDefs ++ proplists:get_value(fields, Configuration, []) ++ [{message, list}], % KV-pairs of field/type definitions
    {Fields, Types} = lists:unzip(FieldDefs),
    Defaults = list_to_tuple([Name|[undefined || _ <- lists:seq(1, length(Fields))]]),
    DoReplicate = proplists:get_value(replicate, Configuration, false),
    io:format("~p ~p ~p ~p~n", [Name, Fields, Types, Defaults]),
    ImemSession:run_cmd(create_table, [Name, {Fields, Types, Defaults}, [{local_content, DoReplicate == false}, {type, bag}], lager_imem]),
    ImemSession:run_cmd(check_table, [Name]).

init(Params) ->
    application:start(imem),
    %try
        State = state_from_params(#state{}, Params),
        Password = erlang:md5(State#state.password),
        Cred = {State#state.user, Password},
        io:format("before ~p~n", [State]),
        ImemSession = erlimem:open(local, {State#state.db}, Cred),
        io:format("after ~p~n", [State]),
        [setup_table(ImemSession, Name, Configuration) || {Name, Configuration} <- State#state.tables ++ [{?MODULE, []}]],
        {ok, State#state{session=ImemSession}}.
    %catch
    %    Class:Reason -> io:format(user, "~p failed with ~p:~p~n", [?MODULE,Class, Reason]),
    %        {stop, "Cant create required lager imem resources"}
    %end.

handle_event({log, LagerMsg}, State = #state{tables=Tables, session=ImemSession, level = LogLevel}) ->
    case lager_util:is_loggable(LagerMsg, LogLevel, ?MODULE) of
        true ->
            Level = lager_msg:severity_as_int(LagerMsg),
            {Date, Time} = lager_msg:timestamp(LagerMsg),
            Message = lager_msg:message(LagerMsg),
            Metadata = lager_msg:metadata(LagerMsg),
            Mod = proplists:get_value(module, Metadata),
            Fun = proplists:get_value(function, Metadata),
            Line = proplists:get_value(line, Metadata),
            Pid = proplists:get_value(pid, Metadata),

            Table = proplists:get_value(imem_table, Metadata, ?MODULE),
            Node = node(),
            Configuration = proplists:get_value(Table, Tables, []),
            FieldData = [proplists:get_value(Field, Metadata) || {Field, _} <- proplists:get_value(fields, Configuration, [])],
            Entry =
                    lists:append([[Table,
                     {lists:flatten(Date), lists:flatten(Time)},
                     lager_util:num_to_level(Level), Pid, Mod, Fun, Line, Node
                                  ], FieldData, [Message]]),

            EntryTuple = list_to_tuple(Entry),
            ImemSession:run_cmd(write, [Table, EntryTuple]);
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
                                     level = OldLevel,
                                     db = OldDb,
                                     tables = OldTables}, Params) ->
    User = proplists:get_value(user, Params, OldUser),
    Password = proplists:get_value(password, Params, OldPassword),
    Db = proplists:get_value(db, Params, OldDb),
    Tables = proplists:get_value(tables, Params, OldTables),
    Level = proplists:get_value(level, Params, OldLevel),

    OrigState#state{level=lager_util:level_to_num(Level),
                    user=User,
                    password=Password,
                    db=Db,
                    tables=Tables}.

%%%===================================================================
%%% Tests
%%%===================================================================

test() ->
    application:load(lager),
    application:set_env(lager, handlers, [{lager_console_backend, debug},
                                          {lager_imem, [{db, "MproLog"},
                                                        {level, debug},
                                                        {user, <<"admin">>},
                                                        {password, <<"change_on_install">>},
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
    lager:log(info, self(), "Test INFO message"),
    lager:log(debug, self(), "Test DEBUG message"),
    lager:log(error, self(), "Test ERROR message"),
    lager:debug([{imem_table, customers}, {key, 123456}, {client_id, "abc"}], "TEST debug message"),
    lager:warning([{a,b}, {c,d}], "Hello", []),
    lager:info("Info ~p", ["variable"]).
