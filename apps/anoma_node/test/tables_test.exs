defmodule TablesTest do
  use ExUnit.Case, async: true
  use TestHelper.TestMacro

  use TestHelper.GenerateExampleTests,
    for: Anoma.Node.Tables
end
