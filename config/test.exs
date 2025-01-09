import Config

config :logger,
  level: :warning

# rocksdb is disabled for testing because it slows tests down too much
config :anoma_node, :mnesia,
  persist_to_disk: false,
  rocksdb: false
