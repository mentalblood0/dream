require "yaml"
require "benchmark"

require "./src/dream.cr"

config = NamedTuple(
  seed: Int32,
  path: String,
  tags_count: UInt32,
  objects_count: UInt32,
  tags_per_object_count: UInt32).from_yaml File.read ARGV.first

rnd = Random.new config[:seed]

tags = Array.new config[:tags_count] { rnd.hex 16 }

ind = Dream::Index.from_yaml <<-YAML
env:
  opts:
    sophia:
      path: /tmp/dream
    db:
      t2o: &ddbs
        compression: zstd
      o2t:
        *ddbs
      i2t:
        *ddbs
      t2i:
        *ddbs
      i2o:
        *ddbs
      o2i:
        *ddbs
YAML

config[:objects_count].times do
  ind.add(
    rnd.random_bytes(16),
    tags[0..rnd.rand(config[:tags_per_object_count]..tags.size.to_u32 - 1)].sample(config[:tags_per_object_count], rnd))
end

Benchmark.ips do |b|
  (1..4).each do |tc|
    limit = 1_u32
    until limit >= config[:objects_count]
      b.report "searching #{limit} objects by #{tc} tags" do
        ind.find tags.sample(tc, rnd), limit: limit
      end
      limit *= 10
    end
  end
end
