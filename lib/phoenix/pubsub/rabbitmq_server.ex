defmodule Phoenix.PubSub.RabbitMQServer do
  use GenServer
  use AMQP
  alias Phoenix.PubSub.RabbitMQ
  alias Phoenix.PubSub.RabbitMQConsumer, as: Consumer
  require Logger

  #@prefetch_count 10

  @moduledoc """
  `Phoenix.PubSub` adapter for RabbitMQ

  See `Phoenix.PubSub.RabbitMQ` for details and configuration options.
  """

  def start_link(server_name, conn_pool_name, pub_pool_name, opts) do
    GenServer.start_link(
      __MODULE__,
      [server_name, conn_pool_name, pub_pool_name, opts],
      name: server_name
    )
  end

  @doc """
  Initializes the server.

  """
  def init([server_name, conn_pool_name, pub_pool_name, opts]) do
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       cons: Map.new(),
       subs: Map.new(),
       conn_pool_name: conn_pool_name,
       pub_pool_name: pub_pool_name,
       exchange: rabbitmq_namespace(server_name),
       node_ref: :crypto.strong_rand_bytes(16),
       opts: opts
     }}
  end

  def handle_call({:subscribe, pid, topic, opts}, _from, state) do
    link = Keyword.get(opts, :link, false)

    has_key =
      case Map.get(state.subs, topic) do
        {pids, size} when size > 0 -> Map.has_key?(pids, pid)
        _ -> false
      end

    unless has_key do
      {:ok, consumer_pid} =
        Consumer.start(state.conn_pool_name, state.exchange, topic, pid, state.node_ref, link)

      Process.monitor(consumer_pid)

      if link, do: Process.link(pid)

      {:reply, :ok,
       %{
         state
         | subs: add_subscriber(state.subs, pid, topic, consumer_pid),
           cons: Map.put(state.cons, consumer_pid, {topic, pid})
       }}
    end
  end

  def handle_call({:unsubscribe, pid, topic}, _from, state) do
    case Map.fetch(state.subs, topic) do
      {:ok, {pids, _size}} ->
        case Map.fetch(pids, pid) do
          {:ok, consumer_pid} ->
            :ok = Consumer.stop(consumer_pid)
            {:reply, :ok, %{state | subs: delete_subscriber(state.subs, pid, topic)}}

          :error ->
            {:reply, :ok, state}
        end

      :error ->
        {:reply, :ok, state}
    end
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:subscribers, topic}, _from, state) do
    case Map.get(state.subs, topic, {Map.new(), 0}) do
      {pids, size} when size > 0 -> {:reply, Map.keys(pids), state}
      {_, 0} -> {:reply, [], state}
    end
  end

  def handle_call({:broadcast, from_pid, topic, msg}, _from, state) do
    case RabbitMQ.publish(
           state.pub_pool_name,
           state.exchange,
           topic,
           :erlang.term_to_binary({state.node_ref, from_pid, msg}),
           content_type: "application/x-erlang-binary"
         ) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    state =
      case Map.fetch(state.cons, pid) do
        {:ok, {topic, sub_pid}} ->
          %{
            state
            | cons: Map.delete(state.cons, pid),
              subs: delete_subscriber(state.subs, sub_pid, topic)
          }

        :error ->
          state
      end

    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    # Ignore subscriber exiting; the Consumer will monitor it
    {:noreply, state}
  end

  defp add_subscriber(subs, pid, topic, consumer_pid) do
    subs
    |> Map.put_new(topic, {Map.new(), 0})
    |> Map.update!(topic, fn {dict, size} ->
      {Map.put_new(dict, pid, consumer_pid), size + 1}
    end)
  end

  defp delete_subscriber(subs, pid, topic) do
    case Map.fetch(subs, topic) do
      {:ok, {pids, size}} ->
        {pids, size} = {Map.delete(pids, pid), size - 1}

        if size > 0 do
          Map.put(subs, topic, pids)
        else
          Map.delete(subs, topic)
        end

      :error ->
        subs
    end
  end

  defp rabbitmq_namespace(server_name) do
    case Atom.to_string(server_name) do
      "Elixir." <> name -> name
      name -> name
    end
  end
end
