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

  defmacro __using__(options) do
    quote do
      import unquote(__MODULE__), only: [task: 2]
      @before_compile unquote(__MODULE__)
      @options unquote(options)
      @task_flow_tmp []
      @default_task_timeout 5_000

      use GenServer

      alias TaskFlow.Utils

      def start_link(args \\ []) do
        args = Map.new(args)
        task_flow = get_all_task_flow()
        server_name = Keyword.get(@options, :server_name, __MODULE__)

        case GenServer.start_link(__MODULE__, {task_flow, args}, name: server_name) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            Process.link(pid)
            {:ok, pid}
        end
      end

      def start_flow(server_name, options \\ [])

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
        new_state = module_handle_start(state)
        send(self(), {Keyword.fetch!(task_flow, :default_entrance)})
        {:reply, :task_started, %{new_state | start_time: start_time, ets: create_resource()}}
      end

      def handle_call(:start_flow, _from, %{start_time: start_time} = state) do
        {:reply, {:task_running, start_time}, module_handle_start(state)}
      end

      def handle_call(
            {:start_flow, options},
            _from,
            %{start_time: nil, task_flow: task_flow} = state
          ) do
        start_time = Time.utc_now()
        new_state = module_handle_start(state)
        entrance = {Keyword.get(options, :entrance, Keyword.fetch!(task_flow, :default_entrance))}
        send(self(), entrance)
        {:reply, :task_started, %{new_state | start_time: start_time, ets: create_resource()}}
      end

      def handle_call({:start_flow, _options}, _from, %{start_time: start_time} = state) do
        {:reply, {:task_running, start_time}, module_handle_start(state)}
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

      def handle_info({nil}, state) do
        new_state = module_handle_over(state)
        maybe_need_return(Map.get(state, :return), {:all_over, state})
        {:noreply, Utils.clear_resource(new_state)}
      end

      def handle_info({:timeout, _, {:task_timeout, one_task, task_pid}}, state) do
        %{ets: %{retry_ets: retry_ets, pid_ets: pid_ets}, task_flow: task_flow} = state

        retry_limit =
          Keyword.get(Keyword.fetch!(task_flow, elem(one_task, 0)), :task_retry_limit, 3)

        Utils.unregister_task_pid(task_pid, pid_ets)
        Utils.kill_task_pid(task_pid)
        Utils.maybe_retry_task(retry_ets, one_task, retry_limit)
        {:noreply, module_handle_task_timeout(one_task, state)}
      end

      def handle_info({task_flag}, %{ets: ets, task_flow: task_flow} = state) do
        %{pid_ets: pid_ets, timer_ets: timer_ets} = state.ets
        new_state = module_handle_flow_start(task_flag, state)

        task_timeout =
          Keyword.get(Keyword.fetch!(task_flow, task_flag), :task_timeout, @default_task_timeout)

        {task_module, task_func, 1} =
          task_flow
          |> Keyword.fetch!(task_flag)
          |> Keyword.fetch!(:task_func)
          |> :erlang.fun_info_mfa()

        pid = Utils.spawn_task_proc(task_module, task_func, [new_state])
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
          |> Keyword.fetch!(task_flag)
          |> Keyword.get(:max_concurrency, System.schedulers_online())

        new_state = module_handle_parent_task_over(task_flag, state)

        new_state =
          if :ets.info(task_ets, :size) == 0 do
            send(self(), {Keyword.get(Keyword.fetch!(task_flow, task_flag), :next)})
            module_handle_flow_over(task_flag, new_state)
          else
            deliver_split_task(task_flag, task_ets, task_ets_tmp, max_concurrency)
            new_state
          end

        {:noreply, new_state}
      end

      def handle_info({task_flag, task_id}, %{ets: ets, task_flow: task_flow} = state) do
        %{pid_ets: pid_ets, timer_ets: timer_ets, task_ets_tmp: task_ets_tmp} = ets
        new_state = module_handle_child_task_start(task_flag, task_id, state)

        task_timeout =
          Keyword.get(Keyword.fetch!(task_flow, task_flag), :task_timeout, @default_task_timeout)

        {task_module, task_func, 2} =
          task_flow
          |> Keyword.fetch!(task_flag)
          |> Keyword.fetch!(:child_task_func)
          |> :erlang.fun_info_mfa()

        task = :ets.lookup(task_ets_tmp, task_id)
        pid = Utils.spawn_task_proc(task_module, task_func, [task, new_state])
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
        new_state = module_handle_child_task_over(task_flag, task_id, state)

        new_state =
          if :ets.info(task_ets, :size) == 0 do
            if :ets.info(task_ets_tmp, :size) == 0 do
              send(self(), {Keyword.get(Keyword.fetch!(task_flow, task_flag), :next)})
              module_handle_flow_over(task_flag, state)
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
              retry_limit =
                task_flow
                |> Keyword.fetch!(elem(one_task, 0))
                |> Keyword.get(:task_retry_limit, 3)

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

      defp module_handle_start(state) do
        if function_exported?(__MODULE__, :handle_start, 1) do
          apply(__MODULE__, :handle_start, [state])
        else
          state
        end
      end

      defp module_handle_over(state) do
        if function_exported?(__MODULE__, :handle_over, 1) do
          apply(__MODULE__, :handle_over, [state])
        else
          state
        end
      end

      defp module_handle_flow_start(task_flag, state) do
        if function_exported?(__MODULE__, :handle_flow_start, 2) do
          apply(__MODULE__, :handle_flow_start, [task_flag, state])
        else
          state
        end
      end

      defp module_handle_flow_over(task_flag, state) do
        if function_exported?(__MODULE__, :handle_flow_over, 2) do
          apply(__MODULE__, :handle_flow_over, [task_flag, state])
        else
          state
        end
      end

      defp module_handle_parent_task_over(task_flag, state) do
        if function_exported?(__MODULE__, :handle_parent_task_over, 2) do
          apply(__MODULE__, :handle_parent_task_over, [task_flag, state])
        else
          state
        end
      end

      defp module_handle_child_task_start(task_flag, task_id, state) do
        if function_exported?(__MODULE__, :handle_child_task_start, 3) do
          apply(__MODULE__, :handle_child_task_start, [task_flag, task_id, state])
        else
          state
        end
      end

      defp module_handle_child_task_over(task_flag, task_id, state) do
        if function_exported?(__MODULE__, :handle_child_task_over, 3) do
          apply(__MODULE__, :handle_child_task_over, [task_flag, task_id, state])
        else
          state
        end
      end

      defp module_handle_task_timeout(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_timeout, 2) do
          apply(__MODULE__, :handle_task_timeout, [one_task, state])
        else
          state
        end
      end

      defp module_handle_task_exit(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_exit, 2) do
          apply(__MODULE__, :handle_task_exit, [one_task, state])
        else
          state
        end
      end

      defp module_handle_task_failed(one_task, state) do
        if function_exported?(__MODULE__, :handle_task_failed, 2) do
          apply(__MODULE__, :handle_task_failed, [one_task, state])
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
        |> Keyword.fetch!(task_flag)
        |> Keyword.get(:exit_on_failed?, true)
      end
    end
  end

  defmacro task(task_name, task_meta) do
    quote do
      @task_flow_tmp Keyword.put(@task_flow_tmp, unquote(task_name), unquote(task_meta))
    end
  end

  defmacro __before_compile__(%Macro.Env{module: module}) do
    task_flow_tmp = Module.get_attribute(module, :task_flow_tmp)

    quote do
      def get_all_task_flow() do
        unquote(Macro.escape(task_flow_tmp))
      end
    end
  end

  @callback handle_start(map()) :: map()

  @callback handle_over(map()) :: map()

  @callback handle_flow_start(Utils.task_flag(), map()) :: map()

  @callback handle_flow_over(Utils.task_flag(), map()) :: map()

  @callback handle_parent_task_over(Utils.task_flag(), map()) :: map()

  @callback handle_child_task_start(Utils.task_flag(), term(), map()) :: map()

  @callback handle_child_task_over(Utils.task_flag(), term(), map()) :: map()

  @callback handle_task_failed(Utils.one_task(), map()) :: map()

  @callback handle_task_timeout(Utils.one_task(), map()) :: map()

  @callback handle_task_exit(Utils.one_task(), map()) :: map()
end
