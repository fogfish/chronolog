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

-compile({parse_transform, category}).
-include("chronolog.hrl").
-include_lib("datum/include/datum.hrl").

-export([start/0]).
-export([
   new/1
,  new/2
,  free/1
,  append/2
,  stream/2
,  stream/3
,  union/1
,  union/2
,  union/3
,  join/1
,  join/2
,  join/3
,  intersect/1
,  intersect/2
,  intersect/3
,  scan/3
]).
-export_type([series/0, range/0]).

%%
%% type definition
-type opts()   :: #{
                     %% The library uses a discrete model of time. It aggregates
                     %% and persists a time series measurement within a chronon.
                     %% The chronon is smallest non decomposable time unit, N milliseconds
                     %% actual length is configurable by application from 1 microsecond to 1 day, 
                     %% default value is 100 ms {0, 0, 100000}
                     chronon => tempus:t()

                     %% The library uses a chunk(s) to combine consequent time series measurements
                     %% Chunks implements a paging approach to manipulate time series data
                     %% default value is 3600 seconds    
                  ,  chunk   => integer()
                  }.

%%
%% A ticker is a short abbreviation used to uniquely identify time series sequence. 
%% The ticker MAY consist of letters numbers or a combination of both, its length MUST NOT exceed 
%% 30 bytes. The ticker MUST be defined and registered by client application.
-type ticker() :: _.

%%
%%
-type series() :: [{tempus:t(), val()} | val()].

-type(fd()     :: any()).
-type(val()    :: integer() | uid:g()).
-type(tag()    :: binary()).
-type(range()  :: {tempus:t(), tempus:t()} | integer()).

%%
%%
start() ->
   application:ensure_all_started(?MODULE).

%%
%% create new time-series ticker 
-spec new(ticker()) -> datum:either( #chronolog{} ).
-spec new(ticker(), opts()) -> datum:either( #chronolog{} ).

new(Ticker) ->
   new(Ticker, #{}).

new(Ticker, Opts) ->
   [either ||
      Chronolog =< #chronolog{
         ticker   = Ticker
      ,  chronon  = lens:get(lens:at(chronon, {0, 0, 100000}), Opts)
      ,  chunk    = lens:get(lens:at(chunk, 3600), Opts)
      },
      supervisor:start_child(chronolog_ticker_sup, [Chronolog]),
      cats:unit(Chronolog)
   ].

%%
%% release resources used by time-series
-spec free(ticker()) -> ok.

free(Ticker) ->
   pipe:free(pns:whereis(chronolog, Ticker)).

%%
%% append value
-spec append(ticker() | #chronolog{}, series()) -> ok.

append(#chronolog{ticker = Ticker} = Chronolog, Stream) ->
   lists:foreach(
      fun({Chunk, SubStream}) -> 
         ok = pts:put(chronolog, {Ticker, Chunk}, SubStream)
      end,
      split(
         Chronolog,
         chronolog_codec:encode(Chronolog, Stream)
      )
   );

append(Ticker, Stream) ->
   append(pipe:ioctl(pns:whereis(chronolog, Ticker), chronolog), Stream).


split(#chronolog{chunk = Length} = Chronolog, [{T0, _} | _] = Stream) ->
   Chunk = chronolog_codec:seconds(T0) div Length,
   {Head, Tail} = lists:splitwith(
      fun({T1, _}) ->
         chronolog_codec:seconds(T1) div Length == Chunk
      end,
      Stream
   ),
   [{Chunk, Head} | split(Chronolog, Tail)];

split(_, []) ->
   [].

%%
%%
-spec stream(ticker() | #chronolog{}, tempus:s()) -> datum:stream(). 
-spec stream(ticker() | #chronolog{}, tempus:t(), tempus:t()) -> datum:stream(). 

stream(Ticker, Sec) ->
   stream(
      Ticker,
      chronolog_codec:t(chronolog_codec:seconds(os:timestamp()) - Sec),
      os:timestamp()
   ).

stream(#chronolog{chunk = Length} = Chronolog, A, B) ->
   Sa = chronolog_codec:seconds(A) div Length,
   Sb = chronolog_codec:seconds(B) div Length,
   Schedule = schedule(A, B, lists:seq(Sa, Sb)),
   stream:unfold(fun unfold/1, {undefined, Schedule, Chronolog});

stream(Ticker, A, B) ->
   stream(pipe:ioctl(pns:whereis(chronolog, Ticker), chronolog), A, B).

schedule(A, B, [Head]) ->
   [{Head, A, B}];

schedule(A, B, [Head | Tail]) ->
   [{Head, A, undefined} | schedule(B, Tail)].

schedule(B, [Head]) ->
   [{Head, undefined, B}];

schedule(B, [Head | Tail]) ->
   [{Head, undefined, undefined} | schedule(B, Tail)].


unfold({undefined, [], _}) ->
   undefined;
unfold({undefined, [{Chunk, A, B} | Tail], #chronolog{ticker = Ticker} = Chronolog}) ->
   case pts:get(chronolog, {Ticker, Chunk, A, B}, infinity) of
      {ok, Heap} ->
         unfold({Heap, Tail, Chronolog});
      {error, _} ->
         unfold({undefined, Tail, Chronolog})
   end;

unfold({[], Tail, Chronolog}) ->
   unfold({undefined, Tail, Chronolog});

unfold({Heap, Tail, Chronolog}) ->
   {hd(Heap), {tl(Heap), Tail, Chronolog}}.
   
%%
%% takes one or more input streams/tickers and returns a newly-allocated
%% stream in which elements united by time property.
-spec union([ticker()], tempus:s()) -> datum:stream().
-spec union([ticker()], tempus:t(), tempus:t()) -> datum:stream().
-spec union([datum:stream()]) -> datum:stream().

union(Tickers, Sec) ->
   union([stream(X, Sec) || X <- Tickers]).

union(Tickers, A, B) ->
   union([stream(X, A, B) || X <- Tickers]).

union(Streams) ->
   do_union(
      lists:sort(
         fun(A, B) -> stream:head(A) =< stream:head(B) end,
         [X || X <- Streams, X =/= ?stream()]
      )
   ).

do_union([]) ->
   stream:new();
do_union([Head | Tail]) ->
   stream:new(
      stream:head(Head),
      fun() -> union([stream:tail(Head) | Tail]) end
   ).

%%
%% takes one or more input streams (tickers) and returns a newly-allocated
%% stream in which each element is a joined by time property of the corresponding 
%% elements of the ticker streams. The output stream is as long as 
%% the longest input stream.
-spec join([ticker()], tempus:s()) -> datum:stream().
-spec join([ticker()], tempus:t(), tempus:t()) -> datum:stream().
-spec join([datum:stream()]) -> datum:stream().

join(Tickers, Sec) ->
   join([stream(X, Sec) || X <- Tickers]).

join(Tickers, A, B) ->
   join([stream(X, A, B) || X <- Tickers]).

join(Streams) ->
   do_join(
      lists:sort(
         fun(A, B) -> stream:head(A) =< stream:head(B) end,
         [X || X <- Streams, X =/= ?stream()]
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
-spec intersect([ticker()], tempus:s()) -> datum:stream().
-spec intersect([ticker()], tempus:t(), tempus:t()) -> datum:stream().
-spec intersect([datum:stream()]) -> datum:stream().

intersect(Tickers, Sec) ->
   intersect([stream(X, Sec) || X <- Tickers]).

intersect(Tickers, A, B) ->
   intersect([stream(X, A, B) || X <- Tickers]).

intersect(Streams) ->
   try
      do_intersect(
         lists:sort(
            fun(A, B) -> stream:head(A) =< stream:head(B) end,
            Streams
         )
      )
   catch _:_ ->
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

scan(_Fun, _Chronon, ?stream()) ->
   ?stream();
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
   stream:new({Chronon, Fun([X || {_, X} <- stream:list(Head)])}, fun() -> scan(Fun, W, Tail) end).
