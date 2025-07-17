require "sophia"

module Dream
  Sophia.define_env DreamEnv, {ii: {key: {ti: UInt32,
                                          oi: UInt32}},
                               i2t: {key: {i2ti: UInt32},
                                     value: {i2tt: String}},
                               t2i: {key: {t2it: String},
                                     value: {t2ii: UInt32}},
                               i2o: {key: {i2oi: UInt32},
                                     value: {i2oo: String}},
                               o2i: {key: {o2io: String},
                                     value: {o2ii: UInt32}},
                               c: {key: {ti: UInt32},
                                   value: {c: UInt32}}}

  class Index
    @tc : UInt32
    @oc : UInt32

    def initialize(path : String, opts : Sophia::H = Sophia::H{"compression"      => "zstd",
                                                               "compaction.cache" => 2_i64 * 1024 * 1024 * 1024})
      @sophia = DreamEnv.new Sophia::H{"sophia.path" => path}, {ii: opts, i2t: opts, t2i: opts, i2o: opts, o2i: opts, c: opts}
      @tc = (@sophia.cursor({i2ti: UInt32::MAX}, "<=").next.not_nil![:i2ti] + 1 rescue 0_u32)
      @oc = (@sophia.cursor({i2oi: UInt32::MAX}, "<=").next.not_nil![:i2oi] + 1 rescue 0_u32)
    end

    def add(object : String, tags : Array(String))
      return if @sophia.has_key?({o2io: object})
      ltc = @tc
      loc = @oc
      @sophia.transaction do |tx|
        tx << {o2io: object, o2ii: loc}
        tx << {i2oi: loc, i2oo: object}
        oi = loc
        loc += 1
        tags.each do |tag|
          ti = (tx[{t2it: tag}]?.not_nil![:t2ii] rescue begin
            tx << {t2it: tag, t2ii: ltc}
            tx << {i2ti: ltc, i2tt: tag}
            tx << {ti: ltc, c: (tx[{ti: ltc}]?.not_nil![:c] rescue 0_u32) + 1}
            ltc += 1
            ltc - 1
          end)
          tx << {ti: ti, oi: oi}
        end
      end
      @tc = ltc
      @oc = loc
    end

    def find(tags : Array(String), limit : UInt32 = UInt32::MAX)
      r = [] of String
      if tags.size == 1
        ti = @sophia[{t2it: tags.first}]?.not_nil![:t2ii] rescue return r
        @sophia.from({ti: ti, oi: 0_u32}) do |ii|
          break if r.size == limit || ii[:ti] != ti
          r << @sophia[{i2oi: ii[:oi]}]?.not_nil![:i2oo]
        end
        return r
      end

      tis = tags
        .map { |t| @sophia[{t2it: t}]?.not_nil![:t2ii] rescue return r }
        .map { |ti| {ti, @sophia[{ti: ti}]?.not_nil![:c]} }
        .sort_by { |ti, c| c }
        .map { |ti, c| ti }

      cs = [] of DreamEnv::IiCursor

      i1 = 0
      i2 = (i1 + 1) % tags.size
      loop do
        if cs.size == tags.size && cs.all? { |c| c.data.not_nil![:oi] == cs.first.data.not_nil![:oi] }
          r << @sophia[{i2oi: cs.first.data.not_nil![:oi]}]?.not_nil![:i2oo]
          return r if r.size == limit
          return r unless cs.first.next && cs.first.data.not_nil![:ti] == tis.first
          i1 = 0
          i2 = (i1 + 1) % tags.size
        end

        if cs.size < tags.size && cs.size <= i1
          c = @sophia.cursor({ti: tis[i1], oi: (cs.last.data.not_nil![:oi] rescue 0_u32)})
          return r unless c.next && c.data.not_nil![:ti] == tis[i1]
          cs << c
        end
        c1 = cs[i1]

        if cs.size < tags.size && cs.size <= i2
          c = @sophia.cursor({ti: tis[i2], oi: (cs.last.data.not_nil![:oi] rescue 0_u32)})
          return r unless c.next && c.data.not_nil![:ti] == tis[i2]
          cs << c
        end
        c2 = cs[i2]

        until c2.data.not_nil![:oi] >= c1.data.not_nil![:oi]
          return r unless c2.next && c2.data.not_nil![:ti] == tis[i2]
        end
        if c2.data.not_nil![:oi] == c1.data.not_nil![:oi]
          i1 = (i1 + 1) % tags.size
          i2 = (i2 + 1) % tags.size
        else
          until cs.first.data.not_nil![:oi] >= cs[i2].data.not_nil![:oi]
            return r unless cs.first.next && cs.first.data.not_nil![:ti] == tis.first
          end
          i1 = 0
          i2 = 1
        end
      end
    end
  end
end
