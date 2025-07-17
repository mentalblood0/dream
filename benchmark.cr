require "yaml"
require "benchmark"

require "./src/dream.cr"

config = NamedTuple(
  path: String,
  tags_count: UInt32,
  objects_count: UInt32,
  tags_per_object_count: UInt32).from_yaml File.read ARGV.first

tags = Array.new config[:tags_count] { Random::DEFAULT.hex 16 }

ind = Dream::Index.new config[:path]
config[:objects_count].times do
  ind.add(
    Random::DEFAULT.hex(16),
    tags[0..Random::DEFAULT.rand(config[:tags_per_object_count]..tags.size.to_u32 - 1)].sample(config[:tags_per_object_count]))
end

Benchmark.ips do |b|
  (1..4).each do |tc|
    limit = 1_u32
    until limit >= config[:objects_count]
      b.report "searching #{limit} objects by #{tc} tags" do
        ind.find tags.sample(tc), limit
      end
      limit *= 10
    end
  end
end
