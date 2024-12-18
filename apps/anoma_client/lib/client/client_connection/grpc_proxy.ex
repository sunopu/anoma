defmodule Anoma.Client.Connection.GRPCProxy do
  use GenServer
  use TypedStruct

  alias Anoma.Protobuf.Intents.Add
  alias Anoma.Protobuf.Intents.List
  alias Anoma.Protobuf.IntentsService
  alias Anoma.Protobuf.Intents.Intent
  require Logger

  ############################################################
  #                    State                                 #
  ############################################################

  typedstruct do
    @typedoc """
    I am the state of a TCP listener.

    My fields contain information to listen for TCP connection with a remote node.

    ### Fields
    - `:port`    - The port on which the remote node is listening to GRPC.
    - `:host`    - The host on which the remote node is listening to GRPC.
    - `:channel` - The channel to the remote grpc.
    """
    field(:port, integer())
    field(:host, String.t())
    field(:channel, any())
  end

  ############################################################
  #                    Genserver Helpers                     #
  ############################################################

  def start_link(args) do
    args = Keyword.validate!(args, [:port, :host])
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(args) do
    state = struct(__MODULE__, Enum.into(args, %{}))

    case GRPC.Stub.connect("#{state.host}:#{state.port}") do
      {:ok, channel} ->
        {:ok, %{state | channel: channel}}

      _err ->
        {:stop, :node_unreachable}
    end
  end

  ############################################################
  #                      Public RPC API                      #
  ############################################################

  @spec list_intents() :: {:ok, List.Response.t()}
  def list_intents() do
    GenServer.call(__MODULE__, {:list_intents})
  end

  @spec add_intent(Intent.t()) :: {:ok, Add.Response.t()}
  def add_intent(intent) do
    GenServer.call(__MODULE__, {:add_intent, intent})
  end

  ############################################################
  #                    Genserver Behavior                    #
  ############################################################

  @impl true
  def handle_call({:list_intents}, _from, state) do
    request = %List.Request{}
    intents = IntentsService.Stub.list_intents(state.channel, request)
    {:reply, intents, state}
  end

  def handle_call({:add_intent, intent}, _from, state) do
    request = %Add.Request{intent: intent}
    result = IntentsService.Stub.add_intent(state.channel, request)
    {:reply, result, state}
  end

  @impl true
  def handle_cast(_message, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(_message, state) do
    {:noreply, state}
  end
end
