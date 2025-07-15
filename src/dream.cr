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

    def find(tags : Array(String), limit : UInt64 = UInt64::MAX)
      r = [] of String

      cs = [] of DreamEnv::TagsCursor
      tags.each do |tag|
        cs << @sophia.cursor({tag: tag, oid: (cs.last.data.not_nil![:oid] rescue "")})
        return r unless cs.last.next
      end

      loop do
        r << cs.first.data.not_nil![:oid] if cs.all? { |c| c.data.not_nil![:oid] == cs.first.data.not_nil![:oid] }
        return r if r.size == limit
        t = cs.first.data.not_nil![:tag]
        loop do
          return r unless cs.first.next && cs.first.data.not_nil![:tag] == t
          break if cs.first.data.not_nil![:oid] >= cs.last.data.not_nil![:oid]
        end
        cs.each_cons_pair do |c1, c2|
          t = c2.data.not_nil![:tag]
          until c2.data.not_nil![:oid] >= c1.data.not_nil![:oid]
            return r unless c2.next && c2.data.not_nil![:tag] == t
          end
        end
      end
      r
    end

    def clear
      @sophia.from({tag: "", oid: ""}) { |rec| @sophia.delete rec }
    end
  end
end
