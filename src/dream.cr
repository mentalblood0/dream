require "sophia"

module Dream
  Sophia.define_env DreamEnv, {tags: {key: {tag: String,
                                            oid: String}}}

  class Index
    def initialize(path : String, opts : Sophia::H = Sophia::H{"compression"      => "zstd",
                                                               "compaction.cache" => 2_i64 * 1024 * 1024 * 1024})
      @sophia = DreamEnv.new Sophia::H{"sophia.path" => path}, {tags: opts}
    end

    def add(oid : String, tags : Array(String))
      @sophia << tags.map { |tag| {tag: tag, oid: oid} } unless @sophia.has_key?({tag: tags.first, oid: oid})
    end

    def find(tag : String)
      r = Set(String).new
      @sophia.from({tag: tag, oid: ""}) do |t|
        break if t[:tag] != tag
        r << t[:oid]
      end
      r
    end

    def find(tags : Array(String))
      tags.map { |tag| find tag }.reduce { |acc, cur| acc &= cur }
    end
  end
end
