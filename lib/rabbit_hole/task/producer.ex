defmodule RabbitHole.Task.Producer do
  @moduledoc """
  Simple task producer API
  """

  use GenServer
  require Logger

  alias RabbitHole.Protocol.{Connection, Channel, Exchange, Basic}
  alias RabbitHole.Task

  @type producer_ref :: any()
  @type exchange :: Exchange.t()
  @type message :: String.t()

  # API

  @spec start(Exchange.t()) :: {:ok, producer_ref()}
  def start(exchange) do
    GenServer.start_link(__MODULE__, exchange)
  end

  @spec stop(producer_ref()) :: :ok
  def stop(ref) do
    GenServer.stop(ref)
  end

  @spec publish(producer_ref(), Task.t(), [Basic.publish_opt()]) :: :ok
  def publish(ref, task, opts \\ []) do
    GenServer.cast(ref, {:publish, %{task: task, opts: opts}})
  end

  # Callbacks

  def init(exchange) do
    # open connection
    {:ok, connection} = Connection.open()
    {:ok, channel} = Channel.open(connection)

    # create exchange
    :ok = Exchange.declare(channel, exchange, :topic)

    # save connection
    {:ok, %{chan: channel, conn: connection, exchange: exchange}}
  end

  def handle_cast({:publish, data}, state) do
    Basic.publish(state.chan, state.exchange, Task.topic(data.task), Task.to_message(data.task), data.opts)
    {:noreply, state}
  end


  def terminate(_reason, state) do
    :ok = Channel.close(state.chan)
    :ok = Connection.close(state.conn)
  end

end
