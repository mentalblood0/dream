require "spec"
require "time"

require "./src/dream.cr"

describe Dream do
  describe "Index" do
    ind = Dream::Index.new "/tmp/dream"
    Spec.before_each do
      ind.clear
    end
    Dream::Index::Strategy.each do |stg|
      it "#{stg}: simple test" do
        ind.add "o1", ["a"]
        ind.add "o2", ["a", "b"]
        ind.add "o3", ["a", "b", "c"]
        ind.find(["a", "b", "c"], 2, stg).should eq ["o3"]
        ind.find(["a", "b"], strategy: stg).should eq ["o2", "o3"]
        ind.find(["a", "b"], 1, stg).should eq ["o2"]
        ind.find(["a"], strategy: stg).should eq ["o1", "o2", "o3"]
        ind.find(["a"], 2, stg).should eq ["o1", "o2"]
        ind.find(["a"], 1, stg).should eq ["o1"]
      end
      it "#{stg}: generative test" do
        tags_count = 20
        objects_count = 1000
        tags_per_object_count = 4
        searches_count = 100

        tags = (1..tags_count).map { Random::DEFAULT.hex 16 }
        objects = Hash(String, Array(String)).zip (1..objects_count).map { Random::DEFAULT.hex 16 }, (1..objects_count).map { tags.sample(tags_per_object_count) }
        searches = (1..searches_count).map { tags.sample 2 }

        objects.each { |oid, tags| ind.add oid, tags }
        searches.each { |tags| ind.find(tags, strategy: stg).each { |oid| (objects[oid] & tags).sort.should eq tags.sort } }
      end
    end
  end
end
