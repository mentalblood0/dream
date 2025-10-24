require "yaml"
require "benchmark"

require "./src/dream.cr"

alias Config = {index: Dream::Index, seed: Int32, tags: Int32, objects: Int32, tags_per_object: Int32, benchmarks: Array(String)}
config = Config.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
random = Random.new config[:seed]
index = config[:index]
index.clear

tags = Array.new config[:tags] { random.random_bytes 16 }

transaction = index.transaction
config[:objects].times do
  transaction.add(
    random.random_bytes(16),
    tags[0..random.rand(config[:tags_per_object]..tags.size - 1)].sample(config[:tags_per_object], random))
end
transaction.commit

if config[:benchmarks].includes? "in-memory"
  Benchmark.ips do |benchmark|
    (1..4).each do |search_tags_count|
      benchmark.report "in-memory: searching all objects by #{search_tags_count} tags" do
        index.find tags.sample search_tags_count, random
      end
    end
  end
end

if config[:benchmarks].includes? "on-disk"
  index.database.checkpoint
  Benchmark.ips do |benchmark|
    (1..4).each do |search_tags_count|
      benchmark.report "on-disk: searching all objects by #{search_tags_count} tags" do
        index.find tags.sample search_tags_count, random
      end
    end
  end
end
