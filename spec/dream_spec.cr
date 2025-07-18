require "spec"
require "time"

require "../src/dream.cr"

describe Dream do
  describe "Index" do
    ind = Dream::Index.new "/tmp/dream"
    it "simple test" do
      ind.add("o1", ["a"])
      ind.add("o2", ["a", "b"])
      ind.add("o3", ["a", "b", "c"])
      ind.find(["a", "b", "c"], 2).should eq ["o3"]
      ind.find(["a", "b"]).should eq ["o2", "o3"]
      ind.find(["a", "b"], 1).should eq ["o2"]
      ind.find(["a"]).should eq ["o1", "o2", "o3"]
      ind.find(["a"], 2).should eq ["o1", "o2"]
      ind.find(["a"], 1).should eq ["o1"]
    end
    it "generative test" do
      tags_count = 20
      objects_count = 1000
      tags_per_object_count = 4
      searches_count = 100

      tags = (1..tags_count).map { Random::DEFAULT.hex 16 }
      objects = Hash.zip (1..objects_count).map { Random::DEFAULT.hex(16) }, (1..objects_count).map { tags.sample(tags_per_object_count) }
      t2o = {} of String => Set(String)
      objects.each { |o, tt| tt.each do |t|
        t2o[t] = Set(String).new unless t2o[t]?
        t2o[t] << o
      end }
      searches = (1..searches_count).map { tags.sample 2 }

      objects.each { |oid, tags| ind.add oid, tags }
      searches.each do |tags|
        r = ind.find(tags).sort
        correct = tags.map { |t| t2o[t] }.reduce { |acc, cur| acc &= cur }.to_a.sort
        r.should eq correct

        r = [] of String
        until (rp = ind.find(tags, 2, (r.last rescue nil))).empty?
          r += rp
        end
        r.sort.should eq correct
      end
    end
  end
end
