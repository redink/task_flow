defmodule TaskFlow2.Example do
  use TaskFlow,
    task_flow: %{
      default_entrance: :flow2,
      flow2: %{
        max_concurrency: 10,
        exit_on_failed?: true,
        task_module: Flow2,
        task_retry_limit: 3,
        task_timeout: 5_000,
        next: :all_over
      }
    },
    server_name: __MODULE__

  def handle_task_start({:flow2}, state) do
    state
    |> Map.put(:flow2_use_ets, :ets.new(:flow2_use_ets, [:public]))
    |> Map.put(:assist_for_retry_times, :ets.new(:assist_for_retry_times, [:public]))
  end

  def handle_task_start(_, state), do: state
end

defmodule TaskFlow22Test do
  use ExUnit.Case

  TaskFlow2.Example.start_link(%{return: self()})
  TaskFlow2.Example.start_flow(TaskFlow2.Example)

  receive do
    {:failed_over, {:can_not_retry, {:flow2, 2}, state}} ->
      %{assist_for_retry_times: assist_for_retry_times} = state

      assert [{{:flow2, 0}, 1}, {{:flow2, 1}, 1}, {{:flow2, 2}, 3}] ==
               assist_for_retry_times
               |> :ets.tab2list()
               |> Enum.sort()

      assert true
  after
    5000 ->
      exit(1)
  end
end
