defmodule FunWithFlags.Store.Cache do
  @moduledoc """
  The in-memory cache for the feature flag, backed by an ETS table.

  This module is not meant to be used directly, but some of its functions can be
  useful to debug flag state.
  """

  @type ttl :: integer
  @type cached_at :: integer

  @doc false
  use GenServer

  alias FunWithFlags.Actor
  alias FunWithFlags.Config
  alias FunWithFlags.Flag
  alias FunWithFlags.Gate
  alias FunWithFlags.Timestamps

  @table_name :fun_with_flags_cache
  @table_options [
    :set, :protected, :named_table, {:read_concurrency, true}
  ]
  @flag_table_options [
    :set, :protected, {:read_concurrency, true}
  ]


  @doc false
  def worker_spec do
    if FunWithFlags.Config.cache? do
      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, []},
        restart: :permanent,
        type: :worker,
      }
    end
  end


  @doc false
  def start_link do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end


  # We lookup without going through the GenServer
  # for concurrency and performance.
  #
  @doc false
  def get(flag_name) do
    case entry(flag_name) do
      {flag, timestamp, ttl} ->
        validate(flag, timestamp, ttl)
      nil ->
        {:miss, :not_found, nil}
    end
  end


  defp validate(flag, timestamp, ttl) do
    if Timestamps.expired?(timestamp, ttl) do
      {:miss, :expired, flag}
    else
      {:ok, flag}
    end
  end


  @doc false
  @spec enabled?(atom, Keyword.t) :: {:ok, boolean} | :miss
  def enabled?(flag_name, options) do
    case :ets.lookup(@table_name, flag_name) do
      [{^flag_name, tid, timestamp, ttl, with_actors}] ->
        if Timestamps.expired?(timestamp, ttl) do
          :miss
        else
          case relevant_gates(tid, with_actors, options) do
            {:ok, gates} ->
              {:ok, Flag.enabled?(%Flag{name: flag_name, gates: gates}, options)}
            :gone ->
              enabled?(flag_name, options)
          end
        end
      _ ->
        :miss
    end
  end


  defp relevant_gates(tid, _with_actors, []) do
    gates_at(tid, [:boolean, :percentage_of_time])
  end

  defp relevant_gates(tid, false, [for: _item]) do
    non_actor_gates(tid)
  end

  defp relevant_gates(tid, true, [for: item]) do
    case gates_at(tid, [{:actor, Actor.id(item)}]) do
      {:ok, []} -> non_actor_gates(tid)
      result -> result
    end
  end


  defp non_actor_gates(tid) do
    gates_at(tid, [:group, :boolean, :percentage_of_actors, :percentage_of_time])
  end


  defp gates_at(tid, kinds) do
    {:ok, Enum.flat_map(kinds, &gates_of_kind(tid, &1))}
  rescue
    ArgumentError -> :gone
  end


  defp gates_of_kind(tid, kind) do
    case :ets.lookup(tid, kind) do
      [{_kind, gates}] -> gates
      [] -> []
    end
  end


  defp entry(flag_name) do
    case :ets.lookup(@table_name, flag_name) do
      [{^flag_name, tid, timestamp, ttl, _with_actors}] ->
        case stored_flag(tid) do
          {:ok, flag} -> {flag, timestamp, ttl}
          :gone -> entry(flag_name)
        end
      [] ->
        nil
    end
  end


  defp stored_flag(tid) do
    [{:flag, flag}] = :ets.lookup(tid, :flag)
    {:ok, flag}
  rescue
    ArgumentError -> :gone
  end


  # We want to always write serially through the
  # GenServer to avoid race conditions.
  #
  @doc false
  def put(flag = %Flag{}) do
    GenServer.call(__MODULE__, {:put, flag})
  end


  @doc """
  Clears the cache. It will be rebuilt gradually as the public interface of the
  package is queried.
  """
  @spec flush() :: true
  def flush do
    GenServer.call(__MODULE__, :flush)
  end


  @doc """
  Returns the flags stored in the cache ETS table, for inspection.
  """
  @spec dump() :: [{atom, {FunWithFlags.Flag.t, cached_at, ttl}}]
  def dump do
    @table_name
    |> :ets.select([{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}])
    |> Enum.flat_map(fn flag_name ->
      case entry(flag_name) do
        {flag, timestamp, ttl} -> [{flag_name, {flag, timestamp, ttl}}]
        nil -> []
      end
    end)
  end


  # ------------------------------------------------------------
  # GenServer callbacks


  @doc false
  def init(:ok) do
    tab_name = @table_name
    ^tab_name = :ets.new(@table_name, @table_options)
    {:ok, %{tab_name: tab_name, ttl: Config.cache_ttl}}
  end


  @doc false
  def handle_call({:put, flag = %Flag{name: name}}, _from, state = %{ttl: ttl}) do
    case :ets.lookup(@table_name, name) do
      [{^name, tid, _timestamp, _ttl, with_actors}] ->
        if :ets.lookup(tid, :flag) == [{:flag, flag}] do
          :ets.insert(@table_name, {name, tid, Timestamps.now, ttl, with_actors})
        else
          publish(flag, ttl)
          :ets.delete(tid)
        end
      [] ->
        publish(flag, ttl)
    end
    {:reply, {:ok, flag}, state}
  end


  @doc false
  def handle_call(:flush, _from, state) do
    tids = :ets.select(@table_name, [{{:_, :"$1", :_, :_, :_}, [], [:"$1"]}])
    result = :ets.delete_all_objects(@table_name)
    Enum.each(tids, &:ets.delete/1)
    {:reply, result, state}
  end


  defp publish(flag = %Flag{name: name, gates: gates}, ttl) do
    tid = :ets.new(:fun_with_flags_flag, @flag_table_options)
    :ets.insert(tid, rows(flag))
    with_actors = Enum.any?(gates, &Gate.actor?/1)
    :ets.insert(@table_name, {name, tid, Timestamps.now, ttl, with_actors})
  end


  defp rows(flag = %Flag{gates: gates}) do
    singles =
      for type <- [:boolean, :percentage_of_time, :percentage_of_actors],
          gate <- [Enum.find(gates, &(&1.type == type))],
          gate != nil,
          do: {type, [gate]}

    groups =
      case Enum.filter(gates, &Gate.group?/1) do
        [] -> []
        group_gates -> [{:group, group_gates}]
      end

    actors =
      gates
      |> Enum.filter(&Gate.actor?/1)
      |> Enum.uniq_by(&(&1.for))
      |> Enum.map(&{{:actor, &1.for}, [&1]})

    [{:flag, flag} | singles ++ groups ++ actors]
  end
end
