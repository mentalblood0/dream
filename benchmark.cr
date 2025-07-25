require "yaml"
require "benchmark"

require "./src/dream.cr"

config = NamedTuple(
  path: String,
  tags_count: UInt32,
  objects_count: UInt32,
  tags_per_object_count: UInt32).from_yaml File.read ARGV.first

tags = Array.new config[:tags_count] { Random::DEFAULT.hex 16 }

opts = Sophia::H{"compression"      => "zstd",
                 "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
env = Dream::Env.new Sophia::H{"sophia.path" => "/tmp/dream"},
  {t2o: opts, o2t: opts, i2t: opts, t2i: opts, i2o: opts, o2i: opts, c: Sophia::H.new}
ind = Dream::Index.new env

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
        ind.find tags.sample(tc), limit: limit
      end
      limit *= 10
    end
  end
end
