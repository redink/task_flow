defmodule Flow1 do
  def flow1(state) do
    "1,2,3\n4,5,6\n7,8,9"
    |> String.split("\n")
    |> Enum.with_index()
    |> Enum.map(fn {v, k} -> {k, v} end)
    |> TaskFlow.add_children_tasks(state)
  end

  def flow1([{task_id, string}], %{flow1_use_ets: flow1_use_ets}) do
    res =
      string
      |> String.split(",")
      |> Enum.map(fn x -> String.to_integer(x) end)
      |> Enum.sum()

    :ets.insert(flow1_use_ets, {task_id, res})
  end
end

defmodule Flow2 do
  def flow2(state) do
    "1,2,3\n4,5,6\n7,8,b"
    |> String.split("\n")
    |> Enum.with_index()
    |> Enum.each(fn {v, k} -> TaskFlow.add_child_task(k, v, state) end)
  end

  def flow2([{task_id, string}], %{
        flow2_use_ets: flow2_use_ets,
        assist_for_retry_times: assist_for_retry_times
      }) do
    :ets.update_counter(assist_for_retry_times, {:flow2, task_id}, 1, {{:flow2, task_id}, 0})

    res =
      string
      |> String.split(",")
      |> Enum.map(fn x -> String.to_integer(x) end)
      |> Enum.sum()

    :ets.insert(flow2_use_ets, {task_id, res})
  end
end

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

defmodule Flow4 do
  def flow4(%{flow4_use_ets: flow4_use_ets} = _state) do
    "1,2,3\n4,5,6\n7,8,b"
    |> String.split("\n")
    |> Enum.with_index()
    |> Enum.each(fn {v, k} -> :ets.insert(flow4_use_ets, {k, v}) end)
  end
end

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
