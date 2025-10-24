require "spec"
require "yaml"
require "benchmark"

require "./src/dream.cr"

alias Config = {index: Dream::Index, seed: Int32, path: String, tags: Int32, objects: Int32, tags_per_object: Int32}
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

Benchmark.ips do |benchmark|
  (1..4).each do |search_tags_count|
    limit = 1
    until limit >= config[:objects]
      benchmark.report "in-memory: searching #{limit} objects by #{search_tags_count} tags" do
        index.find tags.sample(search_tags_count, random), limit: limit
      end
      limit *= 10
    end
  end
end

index.database.checkpoint

Benchmark.ips do |benchmark|
  (1..4).each do |search_tags_count|
    limit = 1
    until limit >= config[:objects]
      benchmark.report "on-disk: searching #{limit} objects by #{search_tags_count} tags" do
        index.find tags.sample(search_tags_count, random), limit: limit
      end
      limit *= 10
    end
  end
end
