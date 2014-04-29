-module(ecql_mnesia_unit).
-export([tests/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

% Basic tests based on http://erlang.org/doc/apps/mnesia/Mnesia_chap2.html#getting_started

-record(employee, {emp_no, name, salary, sex, phone, room_no}).
-record(dept, {id, name}).
-record(project, {name, number}).
-record(manager, {emp, dept}).
-record('at.dep', {emp, dept_id}).
-record('in.proj', {emp, proj_name}).

tests() ->
  {setup, fun start/0, fun stop/1, [
    fun basics/0,
    fun nops/0,
    fun tables/0,
    {generator, fun work/0}
  ]}
.

start() ->
  ecql_mnesia:start()
.

stop(_) ->
  %~ ecql_mnesia:stop()
  ok
.

%% TESTS START HERE

nops() ->
 ?assertMatch(ok, ecql_mnesia:create_schema([node()])),
 ?assertMatch(ok, ecql_mnesia:info())
.

basics() ->
 ?assertMatch(ok, ecql_mnesia:start())
.

tables() ->
  ?assertMatch({atomic, ok}, ecql_mnesia:create_table(employee, [{attributes, record_info(fields, employee)}])),
  ?assertMatch({atomic, ok}, ecql_mnesia:delete_table(employee)),

  ?assertMatch({aborted, _}, ecql_mnesia:create_table(emplo_yee, [{attributes, record_info(fields, employee)}]))
.

work() ->
  {setup,
    fun() ->
      {atomic, ok} = ecql_mnesia:create_table(employee, [{attributes, record_info(fields, employee)}]),
      {atomic, ok} = ecql_mnesia:create_table(dept, [{attributes, record_info(fields, dept)}]),
      {atomic, ok} = ecql_mnesia:create_table(project, [{attributes, record_info(fields, project)}]),
      {atomic, ok} = ecql_mnesia:create_table(manager, [{type, bag}, {attributes, record_info(fields, manager)}]),
      {atomic, ok} = ecql_mnesia:create_table('at.dep', [{attributes, record_info(fields, 'at.dep')}]),
      {atomic, ok} = ecql_mnesia:create_table('in.proj', [{type, bag}, {attributes, record_info(fields, 'in.proj')}])
    end,
    fun(_) ->
      {atomic, ok} = ecql_mnesia:delete_table(employee),
      {atomic, ok} = ecql_mnesia:delete_table(dept),
      {atomic, ok} = ecql_mnesia:delete_table(project),
      {atomic, ok} = ecql_mnesia:delete_table(manager),
      {atomic, ok} = ecql_mnesia:delete_table('at.dep'),
      {atomic, ok} = ecql_mnesia:delete_table('in.proj')
    end,
    fun() ->
      Emp  = #employee{
        emp_no= 104732,
        name = klacke,
        salary = 7,
        sex = male,
        phone = 98108,
        room_no = {221, 015}
      },

      ?assertMatch({atomic, ok}, insert_emp(Emp, 'B/SFR', [erlang, mnesia, otp])),

      ?assertMatch({atomic, ok}, raise(104732, 2)),

      lists:foreach(fun(Table) ->
        lists:foreach(fun(Record) ->
          ecql_mnesia:write(Record)
        end, data(Table))
      end, ecql_mnesia:system_info(tables)),


      {atomic, Women} = all_females(),
      ?assertMatch(["Carlsson Tuula", "Fedoriw Anna"], lists:sort(Women)),

      ?assertMatch(2, ecql_mnesia:match_count(#employee{sex = female, _ = '_'})),

      ?assertEqual(17, sum_salaries()),
      ?assertMatch({atomic, 2}, raise_females(33)),
      ?assertEqual(17 + 2 * 33, sum_salaries())

    end
  }
.

sum_salaries() ->
  ecql_mnesia:foldr(fun(Employee, Total) ->
    Total + Employee#employee.salary
  end, 0, employee)
.

insert_emp(Emp, DeptId, ProjNames) ->
  Ename = Emp#employee.name,
  Fun = fun() ->
    ecql_mnesia:write(Emp),
    AtDep = #'at.dep'{emp = Ename, dept_id = DeptId},
    ecql_mnesia:write(AtDep),
    mk_projs(Ename, ProjNames)
  end,
  ecql_mnesia:transaction(Fun)
.

mk_projs(Ename, [ProjName|Tail]) ->
  ecql_mnesia:write(#'in.proj'{emp = Ename, proj_name = ProjName}),
  mk_projs(Ename, Tail)
;
mk_projs(_, []) ->
  ok
.

raise(Eno, Raise) ->
  F = fun() ->
    [E] = ecql_mnesia:read(employee, Eno, write),
    Salary = E#employee.salary + Raise,
    New = E#employee{salary = Salary},
    ecql_mnesia:write(New)
  end,
  ecql_mnesia:transaction(F)
.

all_females() ->
  F = fun() ->
    Female = #employee{sex = female, name = '$1', _ = '_'},
    ecql_mnesia:select(employee, [{Female, [], ['$1']}])
  end,
  ecql_mnesia:transaction(F)
.

%~ females() ->
  %~ F = fun() ->
    %~ Q = qlc:q([E#employee.name || E <- ecql_mnesia:table(employee),
                %~ E#employee.sex == female]),
    %~ qlc:e(Q)
  %~ end,
  %~ ecql_mnesia:transaction(F)
%~ .

raise_females(Amount) ->
  F = fun() ->
    Female = #employee{sex = female, _ = '_'},
    Fs = ecql_mnesia:select(employee, [{Female, [], ['$_']}]),
    over_write(Fs, Amount)
  end,
  ecql_mnesia:transaction(F).

over_write([E|Tail], Amount) ->
  Salary = E#employee.salary + Amount,
  New = E#employee{salary = Salary},
  ecql_mnesia:write(New),
  1 + over_write(Tail, Amount);
over_write([], _) ->
  0.


%% TEST DATA

data(employee) -> [
  {employee, 104465, "Johnson Torbjorn",   1, male,  99184, {242,038}},
  {employee, 107912, "Carlsson Tuula",     2, female,94556, {242,056}},
  {employee, 114872, "Dacker Bjarne",      3, male,  99415, {221,035}},
  {employee, 104531, "Nilsson Hans",       3, male,  99495, {222,026}},
  {employee, 104659, "Tornkvist Torbjorn", 2, male,  99514, {222,022}},
  {employee, 104732, "Wikstrom Claes",     2, male,  99586, {221,015}},
  {employee, 117716, "Fedoriw Anna",       1, female,99143, {221,031}},
  {employee, 115018, "Mattsson Hakan",     3, male,  99251, {203,348}}
];
data(dept) -> [
  {dept, 'B/SF',  "Open Telecom Platform"},
  {dept, 'B/SFP', "OTP - Product Development"},
  {dept, 'B/SFR', "Computer Science Laboratory"}
];
data(project) -> [
  {project, erlang, 1},
  {project, otp, 2},
  {project, beam, 3},
  {project, mnesia, 5},
  {project, wolf, 6},
  {project, documentation, 7},
  {project, www, 8}
];
data(manager) -> [
  {manager, 104465, 'B/SF'},
  {manager, 104465, 'B/SFP'},
  {manager, 114872, 'B/SFR'}
];
data('at.dep') -> [
  {'at.dep', 104465, 'B/SF'},
  {'at.dep', 107912, 'B/SF'},
  {'at.dep', 114872, 'B/SFR'},
  {'at.dep', 104531, 'B/SFR'},
  {'at.dep', 104659, 'B/SFR'},
  {'at.dep', 104732, 'B/SFR'},
  {'at.dep', 117716, 'B/SFP'},
  {'at.dep', 115018, 'B/SFP'}
];
data('in.proj') -> [
  {'in.proj', 104465, otp},
  {'in.proj', 107912, otp},
  {'in.proj', 114872, otp},
  {'in.proj', 104531, otp},
  {'in.proj', 104531, mnesia},
  {'in.proj', 104545, wolf},
  {'in.proj', 104659, otp},
  {'in.proj', 104659, wolf},
  {'in.proj', 104732, otp},
  {'in.proj', 104732, mnesia},
  {'in.proj', 104732, erlang},
  {'in.proj', 117716, otp},
  {'in.proj', 117716, documentation},
  {'in.proj', 115018, otp},
  {'in.proj', 115018, mnesia}
].




%~ %% Mnesia APIs
%~ -export([
   %~ add_table_copy/3
  %~ ,all_keys/1
  %~ ,change_config/2
  %~ ,change_table_copy_type/3
  %~ ,clear_table/1
  %~ ,create_table/2
  %~ ,delete/1, delete/3
  %~ ,delete_object/1
  %~ ,delete_table/1
  %~ ,dirty_all_keys/1
  %~ ,dirty_delete/1, dirty_delete/3
  %~ ,dirty_delete_object/1
  %~ ,dirty_index_match_object/3, dirty_index_match_object/2
  %~ ,dirty_index_read/3
  %~ ,dirty_match_object/1, dirty_match_object/2, dirty_match_object/3
  %~ ,dirty_read/2, dirty_read/3
  %~ ,dirty_select/1, dirty_select/2, dirty_select/4
  %~ ,dirty_update_counter/3
  %~ ,dirty_write/1
  %~ ,first/1
  %~ ,foldr/3
  %~ ,index_match_object/3, index_match_object/2
  %~ ,index_read/3
  %~ ,load_textfile/1
  %~ ,match_object/1, match_object/2, match_object/3
  %~ ,next/2
  %~ ,read/2, read/3
  %~ ,select/1
  %~ ,select/2
  %~ ,select/4
  %~ ,start/0
  %~ ,stop/0
  %~ ,system_info/1
  %~ ,table_info/2
  %~ ,transaction/1
  %~ ,wait_for_tables/2
  %~ ,write/1

%~ %% Extensions Updating (from ets)
%~ -export([
   %~ update_element/3
%~ ]).


%~ %% Extensions Counting
%~ -export([
   %~ count/2
  %~ ,dirty_count/2
  %~ ,dirty_index_count/3
  %~ ,dirty_index_match_count/3, dirty_index_match_count/2
  %~ ,dirty_match_count/1, dirty_match_count/2
  %~ ,index_count/3
  %~ ,index_match_count/3, index_match_count/2
  %~ ,match_count/1, match_count/2
%~ ]).

%~ %% Extensions Finding (only prim key)
%~ -export([
   %~ dirty_index_key/3
  %~ ,dirty_index_match_key/3, dirty_index_match_key/2
  %~ ,dirty_key/2
  %~ ,dirty_match_key/1, dirty_match_key/2
  %~ ,index_key/3
  %~ ,index_match_key/3, index_match_key/2
  %~ ,key/2
  %~ ,match_key/1, match_key/2
%~ ]).

%~ %% Extensions Traversing (inspired by gb_trees)
%~ -export([
   %~ iterator/1
  %~ ,next/1
%~ ]).

%~ %% Other Extensions
%~ -export([
   %~ select_records/3
%~ ]).
