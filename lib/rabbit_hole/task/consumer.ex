defmodule RabbitHole.Task.Consumer do
  @moduledoc """
  Simple task consumer API
  """

  use GenServer
  require Logger
  alias RabbitHole.Protocol.{Connection, Channel, Queue, Basic, Exchange}
  alias RabbitHole.Task

  @type consumer_ref :: any()
  @type processor :: (Task.t(), consumer_ref -> :ok | :error)
  @type opts() :: [processor: processor()]

  # API

  @spec start(Exchange.t(), Task.kind(), opts()) :: {:ok, consumer_ref()}
  def start(task_exchange, task_kind, opts \\ []) do
    GenServer.start_link(__MODULE__, [{:exchange, task_exchange}, {:task_kind, task_kind} ]++ opts)
  end

  @spec stop(consumer_ref()) :: :ok
  def stop(ref) do
    GenServer.stop(ref)
  end

  # Callbacks

  def init(args) do
    # open connection
    {:ok, connection} = Connection.open()
    {:ok, channel} = Channel.open(connection)

    # create exchange
    :ok = Exchange.declare(channel, args[:exchange], :topic)

    # set up queue
    {:ok, queue} = Queue.declare(channel, "", [:exclusive])
    Queue.bind(channel, queue, args[:exchange], routing_key: Task.topic(args[:task_kind]))
    {:ok, tag} = Basic.consume(channel, queue)

    # save connection
    {:ok,
      %{chan: channel,
        conn: connection,
        exchange: args[:exchange],
        queue: args[:queue],
        tag: tag,
        processor: args[:processor] || &default_processor/1}}
  end

  def handle_info({:basic_deliver, message, _meta}, state) do
    state.processor.(Task.from_message(message), self())
    {:noreply, state}
  end

  def terminate(_reason, state) do
    {:ok, _} = Basic.cancel(state.chan, state.tag)
    :ok = Channel.close(state.chan)
    :ok = Connection.close(state.conn)
  end

  defp default_processor(msg), do: IO.puts("Got message: #{inspect msg}")

end
