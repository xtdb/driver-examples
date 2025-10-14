defmodule TransitExtension do
  @moduledoc """
  Custom Postgrex super extension for handling XTDB's transit type.
  When fallback_output_format=transit is set, XTDB returns a custom 'transit' type
  that contains transit-JSON encoded data.
  """

  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, [send: "transit_send", receive: "transit_recv"]

  def encode(_type_info) do
    quote do
      data ->
        data = Jason.encode!(data)
        [<<IO.iodata_length(data)::signed-size(32)>>, data]
    end
  end

  def decode(_type_info) do
    quote do
      <<len::signed-size(32), text::binary-size(len)>> ->
        case Jason.decode(text) do
          {:ok, decoded} -> TransitExtension.decode_transit_value(decoded)
          {:error, _} -> text
        end
    end
  end

  # Decode transit-JSON structures to native Elixir types (must be public for quote)
  def decode_transit_value(["^ " | rest]) do
    # Transit map: ["^ ", key1, val1, key2, val2, ...]
    rest
    |> Enum.chunk_every(2)
    |> Enum.map(fn [k, v] -> {decode_key(k), decode_transit_value(v)} end)
    |> Map.new()
  end

  def decode_transit_value([tag, value]) when is_binary(tag) do
    # Transit tagged value: [tag, value]
    case tag do
      "~#'" -> value  # Quote - just return the value
      "~t" <> _rest -> value  # Timestamp - return as string
      "~:" <> key -> String.to_atom(key)  # Keyword
      "~#time/zoned-date-time" -> value  # Zoned date-time - return as string
      "~#time/date" -> value  # Date - return as string
      "~#time/instant" -> value  # Instant - return as string
      _ -> [tag, value]  # Unknown tag, return as-is
    end
  end

  def decode_transit_value(list) when is_list(list) do
    Enum.map(list, &decode_transit_value/1)
  end

  def decode_transit_value(value), do: value

  defp decode_key("~:" <> key), do: key
  defp decode_key(key), do: key
end
