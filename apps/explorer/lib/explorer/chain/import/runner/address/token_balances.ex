defmodule Explorer.Chain.Import.Runner.Address.TokenBalances do
  @moduledoc """
  Bulk imports `t:Explorer.Chain.Address.TokenBalance.t/0`.
  """

  require Ecto.Query

  import Ecto.Query, only: [from: 2]

  alias Ecto.{Changeset, Multi, Repo}
  alias Explorer.Chain.Address.TokenBalance
  alias Explorer.Chain.Import
  alias Explorer.Prometheus.Instrumenter

  @behaviour Import.Runner

  # milliseconds
  @timeout 60_000

  @type imported :: [TokenBalance.t()]

  @impl Import.Runner
  def ecto_schema_module, do: TokenBalance

  @impl Import.Runner
  def option_key, do: :address_token_balances

  @impl Import.Runner
  def imported_table_row do
    %{
      value_type: "[#{ecto_schema_module()}.t()]",
      value_description: "List of `t:#{ecto_schema_module()}.t/0`s"
    }
  end

  @impl Import.Runner
  def run(multi, changes_list, %{timestamps: timestamps} = options) do
    insert_options =
      options
      |> Map.get(option_key(), %{})
      |> Map.take(~w(on_conflict timeout)a)
      |> Map.put_new(:timeout, @timeout)
      |> Map.put(:timestamps, timestamps)

    block_numbers = Enum.map(changes_list, & &1.block_number)

    multi
    |> Multi.run(:delete_address_token_balances, fn repo, _ ->
      Instrumenter.block_import_stage_runner(
        fn -> delete_address_token_balances(repo, block_numbers, insert_options) end,
        :block_referencing,
        :token_blances,
        :delete_address_token_balances
      )
    end)
    |> Multi.run(:address_token_balances, fn repo, _ ->
      Instrumenter.block_import_stage_runner(
        fn -> insert(repo, changes_list, insert_options) end,
        :block_referencing,
        :token_blances,
        :address_token_balances
      )
    end)
  end

  @impl Import.Runner
  def timeout, do: @timeout

  defp delete_address_token_balances(repo, block_numbers, %{timeout: timeout}) do
    ordered_query =
      from(tb in TokenBalance,
        where: tb.block_number in ^block_numbers,
        select: map(tb, [:address_hash, :token_contract_address_hash, :token_id, :block_number]),
        # Enforce TokenBalance ShareLocks order (see docs: sharelocks.md)
        order_by: [
          tb.token_contract_address_hash,
          tb.token_id,
          tb.address_hash,
          tb.block_number
        ],
        lock: "FOR UPDATE"
      )

    query =
      from(tb in TokenBalance,
        select: map(tb, [:address_hash, :token_contract_address_hash, :block_number]),
        inner_join: ordered_address_token_balance in subquery(ordered_query),
        on:
          ordered_address_token_balance.address_hash == tb.address_hash and
            ordered_address_token_balance.token_contract_address_hash ==
              tb.token_contract_address_hash and
            ((is_nil(ordered_address_token_balance.token_id) and is_nil(tb.token_id)) or
               (ordered_address_token_balance.token_id == tb.token_id and
                  not is_nil(ordered_address_token_balance.token_id) and not is_nil(tb.token_id))) and
            ordered_address_token_balance.block_number == tb.block_number
      )

    try do
      {_count, deleted_address_token_balances} = repo.delete_all(query, timeout: timeout)

      {:ok, deleted_address_token_balances}
    rescue
      postgrex_error in Postgrex.Error ->
        {:error, %{exception: postgrex_error, block_numbers: block_numbers}}
    end
  end

  @spec insert(Repo.t(), [map()], %{
          optional(:on_conflict) => Import.Runner.on_conflict(),
          required(:timeout) => timeout(),
          required(:timestamps) => Import.timestamps()
        }) ::
          {:ok, [TokenBalance.t()]}
          | {:error, [Changeset.t()]}
  def insert(repo, changes_list, %{timeout: timeout, timestamps: timestamps} = options) when is_list(changes_list) do
    on_conflict = Map.get_lazy(options, :on_conflict, &default_on_conflict/0)

    # Enforce TokenBalance ShareLocks order (see docs: sharelocks.md)
    ordered_changes_list =
      changes_list
      |> Enum.map(fn change ->
        if Map.has_key?(change, :token_id) and Map.get(change, :token_type) == "ERC-1155" do
          change
        else
          Map.put(change, :token_id, nil)
        end
      end)
      |> Enum.group_by(fn %{
                            address_hash: address_hash,
                            token_contract_address_hash: token_contract_address_hash,
                            token_id: token_id,
                            block_number: block_number
                          } ->
        {token_contract_address_hash, token_id, address_hash, block_number}
      end)
      |> Enum.map(fn {_, grouped_address_token_balances} ->
        if Enum.count(grouped_address_token_balances) > 1 do
          Enum.max_by(grouped_address_token_balances, fn balance -> Map.get(balance, :value_fetched_at) end)
        else
          Enum.at(grouped_address_token_balances, 0)
        end
      end)
      |> Enum.sort_by(&{&1.token_contract_address_hash, &1.token_id, &1.address_hash, &1.block_number})

    {:ok, inserted_changes_list} =
      if Enum.count(ordered_changes_list) > 0 do
        Import.insert_changes_list(
          repo,
          ordered_changes_list,
          conflict_target:
            {:unsafe_fragment, ~s<(address_hash, token_contract_address_hash, COALESCE(token_id, -1), block_number)>},
          on_conflict: on_conflict,
          for: TokenBalance,
          returning: true,
          timeout: timeout,
          timestamps: timestamps
        )
      else
        {:ok, []}
      end

    {:ok, inserted_changes_list}
  end

  defp default_on_conflict do
    from(
      token_balance in TokenBalance,
      update: [
        set: [
          value: fragment("COALESCE(EXCLUDED.value, ?)", token_balance.value),
          value_fetched_at: fragment("EXCLUDED.value_fetched_at"),
          token_type: fragment("EXCLUDED.token_type"),
          inserted_at: fragment("LEAST(EXCLUDED.inserted_at, ?)", token_balance.inserted_at),
          updated_at: fragment("GREATEST(EXCLUDED.updated_at, ?)", token_balance.updated_at)
        ]
      ],
      where:
        is_nil(token_balance.value_fetched_at) or fragment("EXCLUDED.value_fetched_at IS NULL") or
          fragment("? < EXCLUDED.value_fetched_at", token_balance.value_fetched_at)
    )
  end
end
