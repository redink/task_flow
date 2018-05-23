defmodule Flow5 do
  def flow5(%{flow5_use_ets: flow5_use_ets, assist_for_retry_times: assist_for_retry_times}) do
    :ets.update_counter(assist_for_retry_times, {:flow5}, 1, {{:flow5}, 0})

    case :ets.lookup(assist_for_retry_times, {:flow5}) do
      [{{:flow5}, retry_times}] when retry_times < 3 ->
        :timer.sleep(10_000)

      _ ->
        "1,2,3\n4,5,6\n7,8,b"
        |> String.split("\n")
        |> Enum.with_index()
        |> Enum.each(fn {v, k} -> :ets.insert(flow5_use_ets, {k, v}) end)
    end
  end
end

defmodule TaskFlow5.Example do
  use TaskFlow,
    task_flow: %{
      flow_entrance: :flow5,
      flow5: %{
        max_concurrency: 10,
        exit_on_failed?: false,
        task_module: Flow5,
        task_retry_limit: 3,
        task_timeout: 1_000,
        next_stage: :all_over
      }
    },
    server_name: __MODULE__

  def handle_task_start({:flow5}, state) do
    case Map.get(state, :assist_for_retry_times) do
      nil ->
        state
        |> Map.put(:flow5_use_ets, :ets.new(:flow5_use_ets, [:public]))
        |> Map.put(:assist_for_retry_times, :ets.new(:assist_for_retry_times, [:public]))

      _ ->
        state
        |> Map.put(:flow5_use_ets, :ets.new(:flow5_use_ets, [:public]))
    end
  end

  def handle_task_start(_, state), do: state
end

defmodule TaskFlow5.Test do
  use ExUnit.Case

  TaskFlow5.Example.start_link(%{return: self()})
  assert :task_started == TaskFlow5.Example.start_flow(TaskFlow5.Example)
  assert {:task_running, _} = TaskFlow5.Example.start_flow(TaskFlow5.Example)

  receive do
    {:all_over, state} ->
      %{flow5_use_ets: flow5_use_ets, assist_for_retry_times: assist_for_retry_times} = state

      assert [{0, "1,2,3"}, {1, "4,5,6"}, {2, "7,8,b"}] ==
               flow5_use_ets
               |> :ets.tab2list()
               |> Enum.sort()

      assert [{{:flow5}, 3}] ==
               assist_for_retry_times
               |> :ets.tab2list()
  after
    5000 ->
      exit(1)
  end
end
