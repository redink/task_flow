defmodule TaskFlow.Utils do
  @moduledoc """
  """

  @type task_flag :: atom()
  @type task_id :: any()
  @type one_task :: {task_flag()} | {task_flag(), task_id()}

  @doc """
  Start global gen server process and link it.
  """
  @spec global_start_link(atom(), {:global, atom()}, any()) :: {:ok, pid()}
  def global_start_link(mod, name, args \\ nil) do
    case GenServer.start_link(mod, args, name: name) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        Process.link(pid)
        {:ok, pid}
    end
  end

  @doc """
  Clear resource which created when started one transform.
  Delete all ets table and set the start time nil and ets nil.
  """
  @spec clear_resource(map()) :: map()
  def clear_resource(%{ets: ets} = state) do
    Enum.each(ets, fn {_, v} -> :ets.delete(v) end)
    %{state | start_time: nil, ets: nil}
  end

  @doc """
  Kill all task process in task pid table.
  """
  @spec clear_resource_kill_pid(map()) :: :ok
  def clear_resource_kill_pid(%{ets: ets}) do
    ets.pid_ets
    |> :ets.tab2list()
    |> Enum.each(fn {pid, _} -> kill_task_pid(pid) end)
  end

  @doc """
  Cancel all timer in timer ets table.
  """
  @spec clear_resource_cancel_timer(map()) :: :ok
  def clear_resource_cancel_timer(%{ets: ets}) do
    ets.timer_ets
    |> :ets.tab2list()
    |> Enum.each(fn {_, timer} -> :erlang.cancel_timer(timer) end)
  end

  @doc """
  Register task pid.
  """
  @spec register_task_pid(pid(), one_task(), :ets.tid()) :: true
  def register_task_pid(pid, one_task, pid_ets) do
    :ets.insert(pid_ets, {pid, one_task})
  end

  @doc """
  Unregister task pid,
  maybe re-call
    - timeout/exit (real process id)
    - do not exit whole flow on one small task failed (`nil`)
  """
  @spec unregister_task_pid(pid(), :ets.tid()) :: true
  def unregister_task_pid(pid, pid_ets) do
    :ets.delete(pid_ets, pid)
  end

  @doc """
  Spawn task process.
  """
  @spec spawn_task_proc(atom(), atom(), list()) :: pid()
  def spawn_task_proc(mod, fun, args) do
    spawn_link(mod, fun, args)
  end

  @doc """
  Kill task process.
  """
  @spec kill_task_pid(pid()) :: true
  def kill_task_pid(task_pid) do
    :erlang.unlink(task_pid)
    Process.exit(task_pid, :kill)
  end

  @doc """
  Start timer for one task.
  """
  @spec start_timer(one_task(), :ets.tid(), pid(), integer()) :: true
  def start_timer(one_task, timer_ets, task_pid, task_timeout \\ 5_000) do
    timer = :erlang.start_timer(task_timeout, self(), {:task_timeout, one_task, task_pid})
    :ets.insert(timer_ets, {one_task, timer})
  end

  @doc """
  Cancel timer for one task.
  """
  @spec cancel_timer(one_task(), :ets.tid()) :: true | nil
  def cancel_timer(one_task, timer_ets) do
    # maybe re-call, task process exit but do not exit whole flow on failed
    case :ets.lookup(timer_ets, one_task) do
      [{^one_task, timer}] ->
        :erlang.cancel_timer(timer)
        :ets.delete(timer_ets, one_task)

      _ ->
        nil
    end
  end

  @doc """
  Get first n item from the ets table.
  """
  @spec first_n(:ets.tid(), integer()) :: [any()]
  def first_n(ets, 1) do
    case :ets.first(ets) do
      :"$end_of_table" -> []
      first -> [first]
    end
  end

  def first_n(ets, n) when n > 1 do
    :ets.safe_fixtable(ets, true)

    case :ets.first(ets) do
      :"$end_of_table" -> []
      first -> first_n(ets, n - 1, first, [first])
    end
  end

  defp first_n(ets, 0, _key, res) do
    :ets.safe_fixtable(ets, false)
    Enum.reverse(res)
  end

  defp first_n(ets, n, key, res) do
    case :ets.next(ets, key) do
      :"$end_of_table" -> Enum.reverse(res)
      next -> first_n(ets, n - 1, next, [next | res])
    end
  end

  @doc """
  Retry task depends on current retry times and retry limit.
  """
  @spec maybe_retry_task(:ets.tid(), one_task(), integer(), boolean()) :: :ok
  def maybe_retry_task(retry_ets, one_task, retry_limit, exit_on_failed? \\ true) do
    current_retry_times = get_current_retry_times(retry_ets, one_task)

    if current_retry_times >= retry_limit do
      if exit_on_failed? do
        send(self(), {:failed_over, {:can_not_retry, one_task}})
      else
        send(self(), Tuple.append(Tuple.insert_at(one_task, 0, :task_over), nil))
      end
    else
      send(self(), one_task)
      incr_retry_times(retry_ets, one_task)
    end

    :ok
  end

  @doc """
  Clear retry times.
  """
  @spec clear_retry(:ets.tid(), tuple()) :: true
  def clear_retry(retry_ets, one_task) do
    :ets.delete(retry_ets, one_task)
  end

  # private
  defp get_current_retry_times(retry_ets, one_task) do
    case :ets.lookup(retry_ets, one_task) do
      [{^one_task, times}] -> times
      _ -> 1
    end
  end

  # private
  defp incr_retry_times(retry_ets, one_task) do
    :ets.update_counter(retry_ets, one_task, 1, {one_task, 1})
  end

  # end of the module
end
