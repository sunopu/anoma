defmodule Anoma.Node.Replay do
  @moduledoc """
  I contain logic to replay storage.

  When a node starts, it can start with an empty database, a "fresh" node.
  If there was pre-existing data present, it's called an an "existing node".

  In case of a fresh node, the node can start up without any ceremony. The database
  is created and the node starts up.

  In case of an existing node, the exising data needs to be verified for correctness.
  The replay mechanism assures that the present data is correct.

  Correct data means that the data in the values and events table is correct and does not lead to
  errors.

  A replay uses the previous data to start up a temporary node with the existing data.
  If that node starts up succesfully, the data is considered valid.
  The real node can continue starting up using the old data.
  The mock node is removed from the system.
  """
  alias Anoma.Node.Tables

  require Logger

  @doc """
  I create a temporary node and return its node id.
  Ensure that the tables have been copied before creating the temporary node.
  """
  def create_temporary_node() do
    node_id = Base.encode64(System.monotonic_time())
  end

  @doc """
  I copy the replay data from a node id to another node id.
  """
  def copy_replay_data(from_node_id, to_node_id) do
    with {:ok, :created} <- Tables.initialize_tables_for_node(to_node_id) do
      IO.puts("tables initialized")
    else
      {:ok, :existing} ->
        Logger.error("data found for replay node #{inspect(to_node_id)}")
        {:error, :target_node_existed}

      {:error, :failed_to_initialize_tables} ->
        Logger.error("failed to create replay node #{inspect(to_node_id)}")
        {:error, :failed_to_create_replay_node}
    end

    # initialize storage for the target node
    case Tables.initialize_tables_for_node(to_node_id) do
      {:ok, :created} ->
        nil
    end
  end

  @doc """
  Given a node id, I try to start the node. If the startup is succesful, I return true.
  If the startup fails, I return false.
  """
  def verify_replay_data(node_id) do
  end

  ############################################################
  #                       Private Helpers                    #
  ############################################################
end
