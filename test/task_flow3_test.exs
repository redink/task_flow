defmodule Flow3 do
  def flow3(state) do
    "1,2,3\n4,5,6\n7,8,b"
    |> String.split("\n")
    |> Enum.with_index()
    |> Enum.each(fn {v, k} -> TaskFlow.add_child_task(k, v, state) end)
  end

  def flow3([{task_id, string}], %{
        flow3_use_ets: flow3_use_ets,
        assist_for_retry_times: assist_for_retry_times
      }) do
    :ets.update_counter(assist_for_retry_times, {:flow3, task_id}, 1, {{:flow3, task_id}, 0})

    res =
      string
      |> String.split(",")
      |> Enum.map(fn x -> String.to_integer(x) end)
      |> Enum.sum()

    :ets.insert(flow3_use_ets, {task_id, res})
  end
end

defmodule TaskFlow3.Example do
  use TaskFlow,
    task_flow: %{
      flow_entrance: :flow3,
      flow3: %{
        max_concurrency: 10,
        exit_on_failed?: false,
        task_module: Flow3,
        task_retry_limit: 3,
        task_timeout: 5_000,
        next_stage: :all_over
      }
    },
    server_name: __MODULE__

  def handle_task_start({:flow3}, state) do
    state
    |> Map.put(:flow3_use_ets, :ets.new(:flow3_use_ets, [:public]))
    |> Map.put(:assist_for_retry_times, :ets.new(:assist_for_retry_times, [:public]))
  end

  def handle_task_start(_, state), do: state
end

defmodule TaskFlow34.Example do
  use TaskFlow,
    task_flow: %{
      flow_entrance: :flow3,
      flow3: %{
        max_concurrency: 10,
        exit_on_failed?: false,
        task_module: Flow3,
        task_retry_limit: 3,
        task_timeout: 5_000,
        next_stage: :flow4
      },
      flow4: %{
        max_concurrency: 10,
        exit_on_failed?: false,
        task_module: Flow4,
        task_retry_limit: 3,
        task_timeout: 5_000,
        next_stage: :all_over
      }
    },
    server_name: __MODULE__

  def handle_task_start({:flow3}, state) do
    state
    |> Map.put(:flow3_use_ets, :ets.new(:flow3_use_ets, [:public]))
    |> Map.put(:assist_for_retry_times, :ets.new(:assist_for_retry_times, [:public]))
  end

  def handle_task_start({:flow4}, state) do
    state
    |> Map.put(:flow4_use_ets, :ets.new(:flow4_use_ets, [:public]))
  end

  def handle_task_start(_, state), do: state
end

defmodule TaskFlow3.Test do
  use ExUnit.Case

  TaskFlow3.Example.start_link(%{return: self()})
  TaskFlow3.Example.start_flow(TaskFlow3.Example)

  receive do
    {:all_over, state} ->
      %{assist_for_retry_times: assist_for_retry_times, flow3_use_ets: flow3_use_ets} = state

      assert [{{:flow3, 0}, 1}, {{:flow3, 1}, 1}, {{:flow3, 2}, 3}] ==
               assist_for_retry_times
               |> :ets.tab2list()
               |> Enum.sort()

      assert [{0, 6}, {1, 15}] ==
               flow3_use_ets
               |> :ets.tab2list()
               |> Enum.sort()
  after
    5000 ->
      exit(1)
  end

  TaskFlow34.Example.start_link(%{return: self()})
  TaskFlow34.Example.start_flow(TaskFlow34.Example)

  receive do
    {:all_over, state} ->
      %{
        assist_for_retry_times: assist_for_retry_times,
        flow3_use_ets: flow3_use_ets,
        flow4_use_ets: flow4_use_ets
      } = state

      assert [{{:flow3, 0}, 1}, {{:flow3, 1}, 1}, {{:flow3, 2}, 3}] ==
               assist_for_retry_times
               |> :ets.tab2list()
               |> Enum.sort()

      assert [{0, 6}, {1, 15}] ==
               flow3_use_ets
               |> :ets.tab2list()
               |> Enum.sort()

      assert [{0, "1,2,3"}, {1, "4,5,6"}, {2, "7,8,b"}] ==
               flow4_use_ets
               |> :ets.tab2list()
               |> Enum.sort()
  after
    5000 ->
      exit(1)
  end
end
