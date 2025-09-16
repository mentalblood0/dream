require "spec"
require "time"

require "../src/dream.cr"

describe Dream do
  describe "Index" do
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

    it "simple test" do
      ind.add("o1".to_slice, ["a"])
      ind.add("o2".to_slice, ["a", "b"])
      ind.add("o3".to_slice, ["a", "b", "c"])

      ind.find(["a", "b", "c"], limit: 2).should eq ["o3".to_slice]
      ind.find(["a", "b"]).should eq ["o2".to_slice, "o3".to_slice]
      ind.find(["a", "b"], limit: 1).should eq ["o2".to_slice]
      ind.find(["a"]).should eq ["o1".to_slice, "o2".to_slice, "o3".to_slice]
      ind.find(["a"], limit: 2).should eq ["o1".to_slice, "o2".to_slice]
      ind.find(["a"], limit: 1).should eq ["o1".to_slice]

      ind.find(["a"], ["a"]).should eq [] of Bytes
      ind.find(["a"], ["b"]).should eq ["o1".to_slice]
      ind.find(["a"], ["c"]).should eq ["o1".to_slice, "o2".to_slice]
      ind.find(["b"], ["a"]).should eq [] of Bytes
      ind.find(["b"], ["c"]).should eq ["o2".to_slice]
      ind.find(["a", "b"], ["c"]).should eq ["o2".to_slice]

      ind.delete "o3".to_slice, ["a", "c"]
      ind.find(["a"]).should eq ["o1".to_slice, "o2".to_slice]
      ind.find(["b"]).should eq ["o2".to_slice, "o3".to_slice]
      ind.find(["c"]).should eq [] of Bytes

      ind.delete "o2".to_slice
      ind.find(["a"]).should eq ["o1".to_slice]
      ind.find(["b"]).should eq ["o3".to_slice]
      ind.find(["c"]).should eq [] of Bytes
    end
    it "generative test", focus: true do
      rnd = Random.new 0

      tags_count = 4
      objects_count = 10
      tags_per_object_count = 2
      searches_count = 10

      tags = (1..tags_count).map { rnd.hex 16 }
      objects = Hash.zip (1..objects_count).map { rnd.random_bytes 16 }, (1..objects_count).map { (tags.sample tags_per_object_count, rnd).sort }
      t2o = {} of String => Set(Bytes)
      objects.each { |o, tt| tt.each do |t|
        t2o[t] = Set(Bytes).new unless t2o[t]?
        t2o[t] << o
      end }
      searches = (1..searches_count).map { tags.sample 2, rnd }

      objects.each { |oid, tags| ind.add oid, tags }
      objects.each { |oid, tags| (ind.get oid).sort.should eq tags }
      searches.each do |tags|
        r = ind.find(tags).sort
        correct = tags.map { |t| t2o[t] }.reduce { |acc, cur| acc &= cur }.to_a.sort
        r.should eq correct

        r = [] of Bytes
        until (rp = ind.find(tags, limit: 2, from: (r.last rescue nil))).empty?
          r += rp
        end
        r.sort.should eq correct
      end
    end
  end
end
