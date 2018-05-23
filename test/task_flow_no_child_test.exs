defmodule Flow4 do
  def flow4(%{flow4_use_ets: flow4_use_ets} = _state) do
    "1,2,3\n4,5,6\n7,8,b"
    |> String.split("\n")
    |> Enum.with_index()
    |> Enum.each(fn {v, k} -> :ets.insert(flow4_use_ets, {k, v}) end)
  end
end

defmodule TaskFlow4.Example do
  use TaskFlow,
    task_flow: %{
      flow_entrance: :flow4,
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

  def handle_task_start({:flow4}, state) do
    state
    |> Map.put(:flow4_use_ets, :ets.new(:flow4_use_ets, [:public]))
  end

  def handle_task_start(_, state), do: state
end

defmodule TaskFlow4.Test do
  use ExUnit.Case

  TaskFlow4.Example.start_link(%{return: self()})
  TaskFlow4.Example.start_flow(TaskFlow4.Example)

  receive do
    {:all_over, state} ->
      %{flow4_use_ets: flow4_use_ets} = state

      assert [{0, "1,2,3"}, {1, "4,5,6"}, {2, "7,8,b"}] ==
               flow4_use_ets
               |> :ets.tab2list()
               |> Enum.sort()
  after
    5000 ->
      exit(1)
  end
end
