defmodule TaskFlow do
  @moduledoc """
  Documentation for TaskFlow.
  """

  @type task_id :: any()
  @type task :: any()

  @spec add_child_task(task_id(), task(), map()) :: :ok
  def add_child_task(task_id, task, %{ets: %{task_ets: task_ets}}) do
    :ets.insert(task_ets, {task_id, task})
    :ok
  end

  @spec add_children_tasks([{task_id(), task()}], map()) :: :ok
  def add_children_tasks(children_tasks, %{ets: %{task_ets: task_ets}}) do
    :ets.insert(task_ets, children_tasks)
    :ok
  end

  defmacro __using__(options) do
    quote bind_quoted: [options: options] do
      @options options
      use GenServer

      alias TaskFlow.Utils

      def start_link(args \\ %{}) do
        task_flow = Keyword.get(@options, :task_flow)
        server_name = Keyword.get(@options, :server_name)

        case GenServer.start_link(__MODULE__, {task_flow, args}, name: server_name) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            Process.link(pid)
            {:ok, pid}
        end
      end

      def start_flow(server_name, options \\ %{})

      def start_flow({:global, server_name}, options) do
        server_name
        |> :global.whereis_name()
        |> GenServer.call({:start_flow, options})
      end

      def start_flow(server_name, options) do
        server_name
        |> GenServer.call({:start_flow, options})
      end

      def init({task_flow, args}) do
        Process.flag(:trap_exit, true)
        {:ok, %{start_time: nil, ets: nil, task_flow: task_flow} |> Map.merge(args)}
      end

      def handle_call(:start_flow, _from, %{start_time: nil, task_flow: task_flow} = state) do
        start_time = Time.utc_now()
        new_state = module_handle_start_flow(state)
        send(self(), {Map.get(task_flow, :default_entrance)})
        {:reply, :task_started, %{new_state | start_time: start_time, ets: create_resource()}}
      end

      def handle_call(:start_flow, _from, %{start_time: start_time} = state) do
        {:reply, {:task_running, start_time}, module_handle_start_flow(state)}
      end

      def handle_call({:start_flow, options}, _from, %{start_time: nil, task_flow: task_flow} = state) do
        start_time = Time.utc_now()
        new_state = module_handle_start_flow(state)
        entrance = {Map.get(options, :entrance, Map.get(task_flow, :default_entrance))}
        send(self(), entrance)
        {:reply, :task_started, %{new_state | start_time: start_time, ets: create_resource()}}
      end

      def handle_call({:start_flow, _options}, _from, %{start_time: start_time} = state) do
        {:reply, {:task_running, start_time}, module_handle_start_flow(state)}
      end

      def handle_call(any, _from, state) do
        {:reply, {:unaccepted, any}, state}
      end

      def handle_cast(_unknown_cast, state) do
        {:noreply, state}
      end

      def handle_info({:failed_over, {:can_not_retry, one_task}}, state) do
        new_state = module_handle_task_failed(one_task, state)
        Utils.clear_resource_kill_pid(new_state)
        Utils.clear_resource_cancel_timer(new_state)

        maybe_need_return(
          Map.get(state, :return),
          {:failed_over, {:can_not_retry, one_task, state}}
        )

        {:noreply, Utils.clear_resource(new_state)}
      end

      def handle_info({:all_over}, state) do
        new_state = module_handle_all_over(state)
        maybe_need_return(Map.get(state, :return), {:all_over, state})
        {:noreply, Utils.clear_resource(new_state)}
      end

      def handle_info({:timeout, _, {:task_timeout, one_task, task_pid}}, state) do
        %{ets: %{retry_ets: retry_ets, pid_ets: pid_ets}, task_flow: task_flow} = state
        retry_limit = Map.get(Map.get(task_flow, elem(one_task, 0)), :task_retry_limit, 3)
        Utils.unregister_task_pid(task_pid, pid_ets)
        Utils.kill_task_pid(task_pid)
        Utils.maybe_retry_task(retry_ets, one_task, retry_limit)
        {:noreply, module_handle_task_timeout(one_task, state)}
      end

      def handle_info({task_flag}, %{ets: ets, task_flow: task_flow} = state) do
        %{pid_ets: pid_ets, timer_ets: timer_ets} = state.ets
        new_state = module_handle_task_start({task_flag}, state)
        task_module = Map.get(Map.get(task_flow, task_flag), :task_module)
        task_timeout = Map.get(Map.get(task_flow, task_flag), :task_timeout)
        pid = Utils.spawn_task_proc(task_module, task_flag, [new_state])
        Utils.register_task_pid(pid, {task_flag}, pid_ets)
        Utils.start_timer({task_flag}, timer_ets, pid, task_timeout)
        {:noreply, new_state}
      end

      def handle_info(
            {:task_over, task_flag, task_pid},
            %{ets: ets, task_flow: task_flow} = state
          ) do
        %{
          task_ets: task_ets,
          task_ets_tmp: task_ets_tmp,
          pid_ets: pid_ets,
          timer_ets: timer_ets,
          retry_ets: retry_ets
        } = ets

        Utils.unregister_task_pid(task_pid, pid_ets)
        Utils.cancel_timer({task_flag}, timer_ets)
        Utils.clear_retry(retry_ets, {task_flag})

        max_concurrency =
          task_flow
          |> Map.get(task_flag)
          |> Map.get(:max_concurrency, System.schedulers_online())

        new_state = module_handle_task_over({task_flag}, state)

        new_state =
          if :ets.info(task_ets, :size) == 0 do
            send(self(), {Map.get(Map.get(task_flow, task_flag), :next)})
            module_handle_task_all_over({task_flag}, new_state)
          else
            deliver_split_task(task_flag, task_ets, task_ets_tmp, max_concurrency)
            new_state
          end

        {:noreply, new_state}
      end

      def handle_info({task_flag, task_id}, %{ets: ets, task_flow: task_flow} = state) do
        %{pid_ets: pid_ets, timer_ets: timer_ets, task_ets_tmp: task_ets_tmp} = ets
        task_module = Map.get(Map.get(task_flow, task_flag), :task_module)
        task_timeout = Map.get(Map.get(task_flow, task_flag), :task_timeout)
        new_state = module_handle_task_start({task_flag, task_id}, state)
        task = :ets.lookup(task_ets_tmp, task_id)
        pid = Utils.spawn_task_proc(task_module, task_flag, [task, new_state])
        Utils.register_task_pid(pid, {task_flag, task_id}, pid_ets)
        Utils.start_timer({task_flag, task_id}, timer_ets, pid, task_timeout)
        {:noreply, new_state}
      end

      def handle_info(
            {:task_over, task_flag, task_id, task_pid},
            %{ets: ets, task_flow: task_flow} = state
          ) do
        %{
          task_ets: task_ets,
          task_ets_tmp: task_ets_tmp,
          pid_ets: pid_ets,
          timer_ets: timer_ets,
          retry_ets: retry_ets
        } = ets

        true = :ets.delete(task_ets_tmp, task_id)
        Utils.unregister_task_pid(task_pid, pid_ets)
        Utils.cancel_timer({task_flag, task_id}, timer_ets)
        Utils.clear_retry(retry_ets, {task_flag, task_id})

        new_state = module_handle_task_over({task_flag, task_id}, state)

        new_state =
          if :ets.info(task_ets, :size) == 0 do
            if :ets.info(task_ets_tmp, :size) == 0 do
              send(self(), {Map.get(Map.get(task_flow, task_flag), :next)})
              module_handle_task_all_over({task_flag}, state)
            else
              new_state
            end
          else
            deliver_split_task(task_flag, task_ets, task_ets_tmp, 1)
            new_state
          end

        {:noreply, new_state}
      end

      def handle_info(
            {:EXIT, pid, :normal},
            %{ets: %{pid_ets: pid_ets}, task_flow: task_flow} = state
          ) do
        case :ets.lookup(pid_ets, pid) do
          [{^pid, {task_flag}}] ->
            send(self(), {:task_over, task_flag, pid})

          [{^pid, {task_flag, task_id}}] ->
            send(self(), {:task_over, task_flag, task_id, pid})

          _ ->
            nil
        end

        {:noreply, state}
      end

      def handle_info({:EXIT, pid, _}, %{ets: ets, task_flow: task_flow} = state) do
        %{pid_ets: pid_ets, retry_ets: retry_ets, timer_ets: timer_ets} = ets

        new_state =
          case :ets.lookup(pid_ets, pid) do
            [{^pid, one_task}] ->
              retry_limit = Map.get(Map.get(task_flow, elem(one_task, 0)), :task_retry_limit, 3)
              Utils.cancel_timer(one_task, timer_ets)
              Utils.unregister_task_pid(pid, pid_ets)

              Utils.maybe_retry_task(
                retry_ets,
                one_task,
                retry_limit,
                exit_on_failed?(one_task, task_flow)
              )

              module_handle_task_exit(one_task, state)

            _ ->
              state
          end

        {:noreply, new_state}
      end

      def handle_info(unknown_info, state) do
        {:noreply, state}
      end

      defp module_handle_start_flow(state) do
        if function_exported?(__MODULE__, :handle_start_flow, 1) do
          __MODULE__.handle_start_flow(state)
        else
          state
        end
      end

      defp module_handle_all_over(state) do
        if function_exported?(__MODULE__, :handle_all_over, 1) do
          __MODULE__.handle_all_over(state)
        else
          state
        end
      end

      defp module_handle_task_start(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_start, 2) do
          __MODULE__.handle_task_start(one_task, state)
        else
          state
        end
      end

      defp module_handle_task_over(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_over, 2) do
          __MODULE__.handle_task_over(one_task, state)
        else
          state
        end
      end

      defp module_handle_task_all_over(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_all_over, 2) do
          __MODULE__.handle_task_all_over(one_task, state)
        else
          state
        end
      end

      defp module_handle_task_timeout(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_timeout, 2) do
          __MODULE__.handle_task_timeout(one_task, state)
        else
          state
        end
      end

      defp module_handle_task_exit(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_exit, 2) do
          __MODULE__.handle_task_exit(one_task, state)
        else
          state
        end
      end

      defp module_handle_task_failed(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_failed, 2) do
          __MODULE__.handle_task_failed(one_task, state)
        else
          state
        end
      end

      defp maybe_need_return(return_pid, return) when is_pid(return_pid) do
        send(return_pid, return)
      end

      defp maybe_need_return(_, _), do: nil

      defp create_resource do
        set_options = [:set, :public]
        bag_options = [:bag, :public]

        %{
          task_ets: :ets.new(:task_ets, bag_options),
          task_ets_tmp: :ets.new(:task_ets_tmp, bag_options),
          retry_ets: :ets.new(:retry_ets, set_options),
          timer_ets: :ets.new(:timer_ets, set_options),
          pid_ets: :ets.new(:pid_ets, set_options)
        }
      end

      defp deliver_split_task(task_flag, main_ets, tmp_ets, n) do
        main_ets
        |> Utils.first_n(n)
        |> Enum.each(fn id ->
          object_list = :ets.lookup(main_ets, id)
          true = :ets.delete(main_ets, id)
          true = :ets.insert(tmp_ets, object_list)
          send(self(), {task_flag, id})
        end)
      end

      defp exit_on_failed?({_task_flag}, _), do: true

      defp exit_on_failed?({task_flag, _task_id}, task_flow) do
        task_flow
        |> Map.get(task_flag)
        |> Map.get(:exit_on_failed?, true)
      end
    end
  end

  @callback handle_start_flow(map()) :: map()

  @callback handle_all_over(map()) :: map()

  @callback handle_task_start(Utils.one_task(), map()) :: map()

  @callback handle_task_over(Utils.one_task(), map()) :: map()

  @callback handle_task_all_over(Utils.one_task(), map()) :: map()

  @callback handle_task_failed(Utils.one_task(), map()) :: map()

  @callback handle_task_timeout(Utils.one_task(), map()) :: map()

  @callback handle_task_exit(Utils.one_task(), map()) :: map()
end
