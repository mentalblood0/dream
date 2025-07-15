require "spec"
require "time"

require "./src/dream.cr"

describe Dream do
  describe "Index" do
    ind = Dream::Index.new "/tmp/dream"
    Spec.before_each do
      ind.clear
    end
    it "simple test" do
      ind.add({0_u64, 1_u64}, ["a"])
      ind.add({0_u64, 2_u64}, ["a", "b"])
      ind.add({0_u64, 3_u64}, ["a", "b", "c"])
      ind.find(["a", "b", "c"], 2).should eq [{0_u64, 3_u64}]
      ind.find(["a", "b"]).should eq [{0_u64, 2_u64}, {0_u64, 3_u64}]
      ind.find(["a", "b"], 1).should eq [{0_u64, 2_u64}]
      ind.find(["a"]).should eq [{0_u64, 1_u64}, {0_u64, 2_u64}, {0_u64, 3_u64}]
      ind.find(["a"], 2).should eq [{0_u64, 1_u64}, {0_u64, 2_u64}]
      ind.find(["a"], 1).should eq [{0_u64, 1_u64}]
    end
    it "generative test" do
      tags_count = 20
      objects_count = 1000
      tags_per_object_count = 4
      searches_count = 100

      tags = (1..tags_count).map { Random::DEFAULT.hex 16 }
      objects = Hash.zip (1..objects_count).map { {Random::DEFAULT.rand(UInt64), Random::DEFAULT.rand(UInt64)} }, (1..objects_count).map { tags.sample(tags_per_object_count) }
      searches = (1..searches_count).map { tags.sample 2 }

      objects.each { |oid, tags| ind.add oid, tags }
      searches.each { |tags| ind.find(tags).each { |oid| (objects[oid] & tags).sort.should eq tags.sort } }
    end
  end
end
