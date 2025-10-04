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
          d2v:
            *ddbs
          c:
            *ddbs
    YAML

    it "simple test" do
      a = "a".to_slice
      b = "b".to_slice
      c = "c".to_slice
      o1 = "o1".to_slice
      o2 = "o2".to_slice
      o3 = "o3".to_slice

      ind.add(o1, [a])
      ind.add(o2, [a, b])
      ind.add(o3, [a, b, c])

      ind.find([a, b, c], limit: 2).map { |i| ind[i]?.not_nil! }.should eq [o3]
      ind.find([a, b]).map { |i| ind[i]?.not_nil! }.should eq [o2, o3]
      ind.find([a, b], limit: 1).map { |i| ind[i]?.not_nil! }.should eq [o2]
      ind.find([a]).map { |i| ind[i]?.not_nil! }.sort.should eq [o1, o2, o3].sort
      ind.find([a], limit: 2).map { |i| ind[i]?.not_nil! }.sort.should eq [o1, o2].sort
      ind.find([a], limit: 1).map { |i| ind[i]?.not_nil! }.should eq [o2]

      ind.find([a], [a]).map { |i| ind[i]?.not_nil! }.should eq [] of Bytes
      ind.find([a], [b]).map { |i| ind[i]?.not_nil! }.should eq [o1]
      ind.find([a], [c]).map { |i| ind[i]?.not_nil! }.sort.should eq [o1, o2].sort
      ind.find([b], [a]).map { |i| ind[i]?.not_nil! }.should eq [] of Bytes
      ind.find([b], [c]).map { |i| ind[i]?.not_nil! }.should eq [o2]
      ind.find([a, b], [c]).map { |i| ind[i]?.not_nil! }.should eq [o2]

      ind.delete o3, [a, c]
      ind.find([a]).map { |i| ind[i]?.not_nil! }.sort.should eq [o1, o2].sort
      ind.find([b]).map { |i| ind[i]?.not_nil! }.should eq [o2, o3]
      ind.find([c]).map { |i| ind[i]?.not_nil! }.should eq [] of Bytes

      ind.delete o2
      ind.find([a]).map { |i| ind[i]?.not_nil! }.should eq [o1]
      ind.find([b]).map { |i| ind[i]?.not_nil! }.should eq [o3]
      ind.find([c]).map { |i| ind[i]?.not_nil! }.should eq [] of Bytes

      ind.delete o1
      ind.delete o3
    end
    it "generative test" do
      rnd = Random.new 0

      tags_count = 8
      objects_count = 100
      tags_per_object_count = 3
      searches_count = 10

      tags = (1..tags_count).map { rnd.random_bytes 16 }
      objects = Hash.zip (1..objects_count).map { rnd.random_bytes 16 }, (1..objects_count).map { (tags.sample tags_per_object_count, rnd).sort }
      t2o = {} of Bytes => Set(Bytes)
      objects.each { |o, tt| tt.each do |t|
        t2o[t] = Set(Bytes).new unless t2o[t]?
        t2o[t] << o
      end }
      searches = (1..searches_count).map { tags.sample 2, rnd }

      objects.each { |oid, tags| ind.add oid, tags }

      objects.each { |oid, tags| (ind.get oid).map { |i| ind[i]?.not_nil! }.sort.should eq tags }
      searches.each do |tags|
        r = ind.find(tags).map { |i| ind[i]?.not_nil! }.sort
        correct = tags.map { |t| t2o[t] }.reduce { |acc, cur| acc &= cur }.to_a.sort
        r.should eq correct

        r = [] of Dream::Id
        until (rp = ind.find(tags, limit: 2_u64, from: (r.last rescue nil))).empty?
          r += rp
        end
        r.map { |i| ind[i]?.not_nil! }.sort.should eq correct
      end
    end
  end
end
