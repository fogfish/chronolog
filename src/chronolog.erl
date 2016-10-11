%%
%%   Copyright (c) 2011 - 2015, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
-module(chronolog).
-include("chronolog.hrl").

-export([start/0]).
-export([
   new/1
  ,free/1
  ,ticker/2
  ,append/3
  ,stream/3
  ,mktag/3
  ,untag/3
  ,match/2
  ,union/1
  ,union/3
  ,join/1
  ,join/3
  ,intersect/1
  ,intersect/3
  ,scan/3
  ,csv/1
]).
-export_type([series/0, range/0]).

%%
%% type definition
-type(fd()     :: any()).
-type(val()    :: integer() | uid:g()).
-type(tag()    :: binary()).
-type(series() :: [{tempus:t(), val()} | val()]).
-type(range()  :: {tempus:t(), tempus:t()} | integer()).

%%
%%
start() ->
   applib:boot(?MODULE, []).

%%
%% create new time-series cask (open existed)
%%   Options:
%%     {file, list()} - filename for time-series
%%     {chronon, tempus:t() | integer()} - time series chronon
-spec new(list()) -> {ok, fd()} | {error, any()}.

new(Opts) ->
   case supervisor:start_child(chronolog_cask_sup, [[{owner, self()}|Opts]]) of
      {ok, Pid} ->
         gen_server:call(Pid, i);
      Error     ->
         Error
   end.

%%
%% release resources used by time-series
-spec free(any()) -> ok.

free(#chronolog{pid = Pid}) ->
   gen_server:call(Pid, free).

%%
%% lookup human readable urn by 64-bit uid
-spec ticker(fd(), uid:l()) -> uri:urn().

ticker(FD, Uid) ->
   chronolog_file:lookup(FD, Uid).

%%
%% append value
-spec append(fd(), uri:uri(), series()) -> {ok, uid:l()}.

append(FD, {urn, _, _}=Urn, Series) ->
   {ok, Uid} = chronolog_file:ticker(FD, Urn),
   lists:foreach(
      fun(X) -> 
         chronolog_file:append(FD, Uid, chronolog_file:encode(FD, X)) 
      end, 
      Series
   ),
   {ok, Uid}.

%%
%% read stream values
-spec stream(fd(), uri:urn(), range()) -> datum:stream().

stream(FD, {urn, _, _}=Urn, {_, _}=Range) ->
   {ok, Uid} = chronolog_file:ticker(FD, Urn),
   chronolog_file:stream(FD, Uid, Range);

stream(FD, {urn, _, _}=Urn, Sec) ->
   T = os:timestamp(),
   stream(FD, Urn, {tempus:sub(T, Sec), T}).

%%
%% create ticker tags
-spec mktag(fd(), uri:urn(), tag()) -> ok.

mktag(FD, {urn, _, _}=Urn, Tag) ->
   {ok, Uid} = chronolog_file:ticker(FD, Urn),
   chronolog_file:mktag(FD, Uid, Tag).

%%
%% remove ticker tags
-spec untag(fd(), uri:urn(), tag()) -> ok.

untag(FD, {urn, _, _}=Urn, Tag) ->
   {ok, Uid} = chronolog_file:ticker(FD, Urn),
   chronolog_file:untag(FD, Uid, Tag).

%%
%% match all ticker to tag
%% @todo: match all tags for ticker
-spec match(fd(), tag()) -> datum:stream().

match(FD, Tag) ->
   chronolog_file:match(FD, Tag).


%%
%% takes one or more input streams (tickers) and returns a newly-allocated
%% stream in which elements united by time property.
-spec union([datum:stream()]) -> datum:stream().
-spec union(fd(), [uri:urn()] | tag(), range()) -> datum:stream().

union(FD, Tag, Range)
 when is_binary(Tag) ->
   union([stream(FD, X, Range) || X <- stream:list(match(FD, Tag))]);

union(FD, Tickers, Range)
 when is_list(Tickers) ->
   union([stream(FD, X, Range) || X <- Tickers]).

union(Streams) ->
   %% sort non-empty streams, so that least time is the first element
   do_union(
      lists:sort(
         fun(A, B) -> stream:head(A) =< stream:head(B) end,
         [X || X <- Streams, X =/= ?NULL]
      )
   ).

do_union([]) ->
   stream:new();
do_union([Head | Tail]) ->
   stream:new(
      stream:head(Head),
      fun() -> 
         union([stream:tail(Head) | Tail]) 
      end
   ).

%%
%% takes one or more input streams (tickers) and returns a newly-allocated
%% stream in which each element is a joined by time property of the corresponding 
%% elements of the ticker streams. The output stream is as long as 
%% the longest input stream.
-spec join([datum:stream()]) -> datum:stream().
-spec join(fd(), [uri:urn()] | tag(), range()) -> datum:stream().

join(FD, Tag, Range)
 when is_binary(Tag) ->
   join([stream(FD, X, Range) || X <- stream:list(match(FD, Tag))]);

join(FD, Tickers, Range)
 when is_list(Tickers) ->
   join([stream(FD, X, Range) || X <- Tickers]).

join(Streams) ->
   %% sort non-empty streams, so that least time is the first element
   do_join(
      lists:sort(
         fun(A, B) -> stream:head(A) =< stream:head(B) end,
         [X || X <- Streams, X =/= ?NULL]
      )
   ).

do_join([]) ->
   stream:new();
do_join(Streams) ->
   {T, _} = stream:head(hd(Streams)),
   {Head, Tail} = lists:splitwith(
      fun(X) -> erlang:element(1, stream:head(X)) =:= T end,
      Streams
   ),
   stream:new(
      {T, [erlang:element(2, stream:head(X)) || X <- Head]}, 
      fun() -> 
         join(lists:foldl(fun(X, Acc) -> [stream:tail(X) | Acc] end, Tail, Head)) 
      end
   ).

%%
%% takes one or more input tickers and returns a newly-allocated stream
%% in which each element is a intersection (inner join) by time property 
%% of the the corresponding elements of the ticker streams. The output 
%% stream is as long as the shortest input stream.
-spec intersect([datum:stream()]) -> datum:stream().
-spec intersect(fd(), [uri:urn()] | tag(), range()) -> datum:stream().

intersect(FD, Tag, Range)
 when is_binary(Tag) ->
   intersect([stream(FD, X, Range) || X <- stream:list(match(FD, Tag))]);

intersect(FD, Tickers, Range)
 when is_list(Tickers) ->
   intersect([stream(FD, X, Range) || X <- Tickers]).

intersect(Streams) ->
   %% sort non-empty streams, so that least time is the first element
   try
      do_intersect(
         lists:sort(
            fun(A, B) -> stream:head(A) =< stream:head(B) end,
            Streams
         )
      )
   catch _:_ ->
      %% stream fail if eof
      stream:new()
   end.

do_intersect([]) ->
   stream:new();
do_intersect(Streams) ->
   {T, _} = stream:head(hd(Streams)),
   {Head, Tail} = lists:splitwith(
      fun(X) -> erlang:element(1, stream:head(X)) =:= T end,
      Streams
   ),
   case length(Head) of
      1 ->
         intersect(lists:foldl(fun(X, Acc) -> [stream:tail(X) | Acc] end, Tail, Head));
      _ ->
         stream:new(
            {T, [erlang:element(2, stream:head(X)) || X <- Head]}, 
            fun() -> 
               intersect(lists:foldl(fun(X, Acc) -> [stream:tail(X) | Acc] end, Tail, Head)) 
            end
         )
   end.

%%
%% accumulates the partial folds of an input stream into a newly-allocated
%% output stream over time. The function aggregates values that belongs to
%% chronon of size W.
-spec scan(function(), tempus:t(), datum:stream()) -> datum:stream().

scan(_Fun, _Chronon, ?NULL) ->
   ?NULL;
scan(Fun, W, Stream)
 when is_integer(W) ->
   scan(Fun, tempus:t(s, W), Stream);
scan(Fun, {_, _, _}=W, Stream) ->
   {T, _}  = stream:head(Stream),
   Chronon = tempus:discrete(T, W),
   {Head, Tail} = stream:splitwhile(
      fun({T1, _}) ->
         tempus:discrete(T1, W) =:= Chronon
      end,
      Stream
   ),
   stream:new({Chronon, Fun([X || {_, X} <- Head])}, fun() -> scan(Fun, W, Tail) end).

%%
%% time series read from csv file and fold it using urn as key
-spec csv(datum:stream()) -> datum:stream().

csv(Stream) ->
   fold(stream:filter(fun assert/1, csv:stream(Stream))).

assert([<<"urn:", _/binary>>, _, _]) ->
   true;
assert(_) ->
   false.

fold({s, [Urn | _], _}=Stream) ->
   reduce(uri:new(Urn),
      stream:splitwhile(
         fun([X | _]) -> X =:= Urn end,
         Stream
      )
   );
fold({}) ->
   stream:new().

reduce(Urn, {Acc, {s, _, _} = Stream}) ->
   stream:new({Urn, lists:reverse(Acc)}, fun() -> fold(stream:tail(Stream)) end);

reduce(Urn, {Acc, {}}) ->
   stream:new({Urn, lists:reverse(Acc)}).

