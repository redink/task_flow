defmodule TaskFlow1.Example do
  use TaskFlow, server_name: __MODULE__

  task :default_entrance, :flow1

  task :flow1,
    max_concurrency: 10,
    exit_on_failed?: true,
    task_module: Flow1,
    task_retry_limit: 3,
    task_timeout: 5_000,
    next: :all_over

  def handle_task_start({:flow1}, state) do
    state
    |> Map.put(:flow1_use_ets, :ets.new(:flow1_use_ets, [:public]))
  end

  def handle_task_start(_, state), do: state
end

defmodule TaskFlow12.Example do
  use TaskFlow, server_name: {:global, __MODULE__}
  task :default_entrance, :flow1

  task :flow1,
    max_concurrency: 10,
    exit_on_failed?: true,
    task_module: Flow1,
    task_retry_limit: 3,
    task_timeout: 5_000,
    next: :flow2

  task :flow2,
    max_concurrency: 10,
    exit_on_failed?: true,
    task_module: Flow2,
    task_retry_limit: 3,
    task_timeout: 5_000,
    next: :all_over

  def handle_task_start({:flow1}, state) do
    state
    |> Map.put(:flow1_use_ets, :ets.new(:flow1_use_ets, [:public]))
  end

  def handle_task_start({:flow2}, state) do
    state
    |> Map.put(:flow2_use_ets, :ets.new(:flow2_use_ets, [:public]))
    |> Map.put(:assist_for_retry_times, :ets.new(:assist_for_retry_times, [:public]))
  end

  def handle_task_start(_, state), do: state
end

defmodule TaskFlow1Test do
  use ExUnit.Case

  TaskFlow1.Example.start_link(return: self())
  TaskFlow1.Example.start_flow(TaskFlow1.Example)

  receive do
    {:all_over, state} ->
      %{flow1_use_ets: flow1_use_ets} = state

      assert [{0, 6}, {1, 15}, {2, 24}] ==
               flow1_use_ets
               |> :ets.tab2list()
               |> Enum.sort()
  after
    5000 ->
      exit(1)
  end

  TaskFlow12.Example.start_link(return: self())
  TaskFlow12.Example.start_flow({:global, TaskFlow12.Example}, entrance: :flow2)

  receive do
    {:failed_over, {:can_not_retry, {:flow2, 2}, state}} ->
      %{assist_for_retry_times: assist_for_retry_times} = state

      assert [{{:flow2, 0}, 1}, {{:flow2, 1}, 1}, {{:flow2, 2}, 3}] ==
               assist_for_retry_times
               |> :ets.tab2list()
               |> Enum.sort()

      assert nil == Map.get(state, :flow1_use_ets)
      assert true
  after
    5000 ->
      exit(1)
  end
end
