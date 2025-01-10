defmodule Anoma.Examples.Tables do
  @moduledoc """
  I define examples on how to use the Tables module to reason about the database
  for nodes.
  """

  alias Anoma.Node.Tables
  alias Anoma.Node.Examples.ENode

  import ExUnit.Assertions

  @doc """
  I test that for a non-existing node, no tables exist.
  """
  def check_for_tables() do
    non_existing_node_id = ENode.random_node_id()
    refute(Tables.existing_tables?(non_existing_node_id))
  end
end
