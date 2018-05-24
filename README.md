# TaskFlow

Why we need another task management flow?

Assuming that we have some scheduled tasks to execute, maybe include:

- fetch data from web server (#1)
- process the data in some specific format roughly (#2)

and we want combine these two steps into one task.

- process the data further (#3)
- write the parsed data into mongo database (#4)

we also want combine these two steps into one task.

Let's continue assume, #1 need one line of code, but it's very likely to fail due to timeout or other reason (retry will very helpful here). After fetched the data, we will parse it to many small contents, and want process the many contents in parallel.

#3 and #4 are the same.

## Features

The main features include:

- customize task flow, need follow DAG
- support retry and retry times limit
- support timeout and kill timeout task process
- support process tasks in parallel as far as possible

## Usage

### define task flow

A typical task flow should be like this:

```elixir
    task_flow: %{
      default_entrance: :flow1,
      flow1: %{
        max_concurrency: 10,
        exit_on_failed?: true,
        task_module: Flow1,
        task_retry_limit: 3,
        task_timeout: 5_000,
        next: :flow2
      },
      flow2: %{
        max_concurrency: 10,
        exit_on_failed?: true,
        task_module: Flow2,
        task_retry_limit: 3,
        task_timeout: 5_000,
        next: :all_over
      }
    }
```

- `default_entrance` is entrance of the flow
- `max_concurrency` define how many concurrent processes to process the task
- `exit_on_failed?` if the whole task failed when one small task exit
- `task_module` define which module to execute this task
- `task_retry_limit` define retry times limit for this task
- `task_timeout` define timeout value for this task
- `next` define the next task

### use `TaskFlow`

The main subjec of `TaskFlow` is Macro, so need to:

```elixir
defmodule TaskFlow1.Example do
  use TaskFlow,
    task_flow: %{
      default_entrance: :flow1,
      flow1: %{
        max_concurrency: 10,
        exit_on_failed?: true,
        task_module: Flow1,
        task_retry_limit: 3,
        task_timeout: 5_000,
        next: :all_over
      }
    },
    server_name: __MODULE__
end
```

`server_name` will determine name for the GenServer process, it supports local mode and global mode.

```elixir
server_name: __MODULE__
```

```elixir
server_name: {:global, __MODULE__}
```

### the function to execute task

For the task `flow1`, we need to define function to execute it. And user need define `Flow1.flow1/1` to execute this task.

```elixir
  def flow1(%{flow1_use_ets: flow1_use_ets} = _state) do
    "1,2,3\n4,5,6\n7,8,b"
    |> String.split("\n")
    |> Enum.with_index()
    |> Enum.each(fn {v, k} -> :ets.insert(flow1_use_ets, {k, v}) end)
  end
```

`state` is the GenServer's state, its struct is:

```elixir
%{start_time: nil, ets: nil, task_flow: task_flow}
```

`ets` includes:

- task_ets, put all sub small tasks
- task_ets_tmp, the assistant for `task_ets`
- retry_ets, record retry times for one task
- timer_ets, record timer ref for one task
- pid_ets, record task executor for one task

Beside this, user also append other fields when execute callback functions, just like:

```elixir
  def handle_task_start({:flow1}, state) do
    state
    |> Map.put(:flow1_use_ets, :ets.new(:flow1_use_ets, [:public]))
  end

  def handle_task_start(_, state), do: state
```

#### children task

For one task, if its results are other task's resource, just like #1 and #2, user can define the #2 is children of #1. When process the task #1, can add child/children task(s).

```elixir
  def flow1(state) do
    "1,2,3\n4,5,6\n7,8,9"
    |> String.split("\n")
    |> Enum.with_index()
    |> Enum.map(fn {v, k} -> {k, v} end)
    |> TaskFlow.add_children_tasks(state)
  end
```

If this, user need also define `Flow1.flow1/2`:

```elixir
  def flow1([{task_id, string}], %{flow1_use_ets: flow1_use_ets}) do
    res =
      string
      |> String.split(",")
      |> Enum.map(fn x -> String.to_integer(x) end)
      |> Enum.sum()

    :ets.insert(flow1_use_ets, {task_id, res})
  end
```

The `TaskFlow` will traverse all children tasks by `task_id`, and the `task_id` is defined by user.

In this case, when process the main task, it added children tasks `{k, v}` is the task id and task definition. When the `TaskFlow` traverse all children, the `flow1/2` function need to accept two parameters, the first is one task id and its all task definition, and second parameter is `state` (GenServer's state).

### callback

`TaskFlow` defined some callback interfaces, which allow user add customized logic, includes:

- handle_start_flow

  When started the whole flow, parameter is: `state`

- handle_all_over

  The whole flow over successfully, parameter is: `state`

- handle_task_start

  When started one task, parameters is: `one task` and `state`

- handle_task_over

  When finished one task, parameters is: `one task` and `state`

- handle_task_all_over

  When finished one task includes its children tasks if there are, parameters is: `one task` and `state`

- handle_task_failed

  One task failed after retry, parameters is: `one task` and `state`

- handle_task_timeout

  One task timeout before retry, parameters is: `one task` and `state`

- handle_task_exit

  One task's executor exit before retry, parameters is: `one task` and `state`

### start

#### start GenServer

As the worker process, user can put the GenServer under the supervisor same as other GenServer. But if user want start the GenServer out of the superviosr, another parameter will be helpful in some cases.

```elixir
TaskFlow1.Example.start_link(%{return: self()})
```

After the whole flow executed over, it will send the result to the `return` process.

if failed over, the result will be:

```elixir
{:failed_over, {:can_not_retry, one_task, state}}
```

if over successfully, the result will be:

```elixir
{:all_over, state}
```

#### start flow

If want to start the flow, could call:

```elixir
TaskFlow1.Example.start_flow(TaskFlow1.Example)
```

`TaskFlow1.Example` is the server name.

## Use cases

See [test](./test)
