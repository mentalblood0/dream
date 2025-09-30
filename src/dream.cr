require "yaml"

require "sophia"
require "xxhash128"

module Dream
  Sophia.define_env Env, {t2o: {key: {t2ot0: UInt64,
                                      t2ot1: UInt64,
                                      t2oo0: UInt64,
                                      t2oo1: UInt64}},
                          o2t: {key: {o2to0: UInt64,
                                      o2to1: UInt64,
                                      o2tt0: UInt64,
                                      o2tt1: UInt64}},
                          d2v: {key: {d2vd0: UInt64,
                                      d2vd1: UInt64},
                                value: {d2vv: Bytes}},
                          c: {key: {ti0: UInt64,
                                    ti1: UInt64},
                              value: {c: UInt64}}}

  alias Id = {UInt64, UInt64}

  struct T2o
    getter tup : {t2ot0: UInt64, t2ot1: UInt64, t2oo0: UInt64, t2oo1: UInt64}

    def initialize(@tup)
    end

    def initialize(t : Id, o : Id)
      @tup = {t2ot0: t[0], t2ot1: t[1], t2oo0: o[0], t2oo1: o[1]}
    end

    def t
      {@tup[:t2ot0], @tup[:t2ot1]}
    end

    def o
      {@tup[:t2oo0], @tup[:t2oo1]}
    end

    def >=(another : T2o)
      (@tup[:t2oo0] >= another.tup[:t2oo0]) ||
        ((@tup[:t2oo0] == another.tup[:t2oo0]) && (@tup[:t2oo1] >= another.tup[:t2oo1]))
    end
  end

  class Index
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter env : Env

    @[YAML::Field(ignore: true)]
    property intx = false

    def initialize(@env : Env)
    end

    def transaction(&)
      if @intx
        yield self
      else
        @env.transaction do |tx|
          r = Index.new tx
          r.intx = true
          yield r
        end
      end
    end

    protected def digest(s : Bytes)
      d = LibXxhash.xxhash128 s, s.size, 0
      {d.high64, d.low64}
    end

    protected def memoize(i : Id, v : Bytes)
      @env << {d2vd0: i[0], d2vd1: i[1], d2vv: v} unless @env[{d2vd0: i[0], d2vd1: i[1]}]?
    end

    protected def forget(i : Id)
      @env.delete({d2vd0: i[0], d2vd1: i[1]})
    end

    protected def value(i : Id)
      @env[{d2vd0: i[0], d2vd1: i[1]}]?.not_nil![:d2vv] rescue nil
    end

    def add(object : Bytes, tags : Array(Bytes))
      transaction do |tx|
        oi = digest object
        tx.memoize oi, object
        tags.each do |tag|
          ti = digest tag
          tx.memoize ti, tag
          tx.env << {t2ot0: ti[0], t2ot1: ti[1], t2oo0: oi[0], t2oo1: oi[1]}
          tx.env << {o2to0: oi[0], o2to1: oi[1], o2tt0: ti[0], o2tt1: ti[1]}
          tx.env << {ti0: ti[0], ti1: ti[1], c: (@env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] rescue 0_u64) + 1}
        end
      end
    end

    def get(object : Bytes, &)
      oi = digest object
      @env.from({o2to0: oi[0], o2to1: oi[1], o2tt0: 0_u64, o2tt1: 0_u64}) do |o2t|
        break unless {o2t[:o2to0], o2t[:o2to1]} == oi
        ti = {o2t[:o2tt0], o2t[:o2tt1]}
        yield (value ti).not_nil!.clone
      end
    end

    def get(object : Bytes) : Array(Bytes)
      r = [] of Bytes
      get(object) { |t| r << t }
      r
    end

    def delete(object : Bytes)
      oi = digest object
      transaction do |tx|
        @env.from({o2to0: oi[0], o2to1: oi[1], o2tt0: 0_u64, o2tt1: 0_u64}) do |o2t|
          break unless {o2t[:o2to0], o2t[:o2to1]} == oi
          ti = {o2t[:o2tt0], o2t[:o2tt1]}
          tx.env.delete({t2ot0: ti[0], t2ot1: ti[1], t2oo0: oi[0], t2oo1: oi[1]})
          tx.env.delete({o2to0: ti[0], o2to1: ti[1], o2tt0: oi[0], o2tt1: oi[1]})
          tx.env << {ti0: ti[0], ti1: ti[1], c: (@env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] - 1 rescue 0_u64)}
        end
        tx.forget oi
      end
    end

    def delete(object : Bytes, tags : Array(Bytes))
      oi = digest object
      transaction do |tx|
        tags.each do |t|
          ti = digest t
          tx.env.delete({t2ot0: ti[0], t2ot1: ti[1], t2oo0: oi[0], t2oo1: oi[1]})
          tx.env.delete({o2to0: oi[0], o2to1: oi[1], o2tt0: ti[0], o2tt1: ti[1]})
          tx.env << {ti0: ti[0], ti1: ti[1], c: (@env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] - 1 rescue 0_u64)}
        end
      end
      transaction do |tx|
        @env.from({o2to0: oi[0], o2to1: oi[1], o2tt0: 0_u64, o2tt1: 0_u64}) do |o2t|
          if {o2t[:o2to0], o2t[:o2to1]} == oi
            return
          else
            break
          end
        end
        forget oi
      end
    end

    def find(present : Array(Bytes), absent : Array(Bytes) = [] of Bytes, from : Bytes? = nil, &)
      fromi = from ? (digest from) : nil

      ais = absent.compact_map { |t| digest t }
      ais.sort_by! { |ti| @env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] }
      ais.reverse!

      if present.size == 1
        ti = digest present.first
        @env.from((T2o.new ti, (fromi ? fromi : {0_u64, 0_u64})).tup, ">") do |t2o|
          break if {t2o[:t2ot0], t2o[:t2ot1]} != ti
          yield value({t2o[:t2oo0], t2o[:t2oo1]}).not_nil!.clone if ais.all? { |ai| !@env.has_key? (T2o.new ai, (T2o.new t2o).o).tup }
        end
        return
      end

      pis = present.map { |t| digest t }
      pis.sort_by! { |ti| @env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] rescue return }

      cs = [] of Env::T2oCursor

      i1 = 0
      i2 = 1
      loop do
        if cs.size == present.size && cs.all? { |c| (T2o.new c.data.not_nil!).o == (T2o.new cs.first.data.not_nil!).o }
          if ais.all? { |ai| !@env.has_key? (T2o.new ai, (T2o.new cs.first.data.not_nil!).o).tup }
            yield value({cs.first.data.not_nil![:t2oo0], cs.first.data.not_nil![:t2oo1]}).not_nil!.clone
          end
          return unless cs.first.next && ((T2o.new cs.first.data.not_nil!).t == pis.first)
          i1 = 0
          i2 = 1
        end

        if cs.size < present.size && cs.size <= i1
          if i1 == 0
            c = @env.cursor((T2o.new pis[i1], (fromi ? fromi : {0_u64, 0_u64})).tup, ">")
          else
            c = @env.cursor((T2o.new pis[i1], (T2o.new cs.last.data.not_nil!).o).tup)
          end
          return unless c.next && ((T2o.new c.data.not_nil!).t == pis[i1])
          cs << c
        end
        c1 = cs[i1]

        if cs.size < present.size && cs.size <= i2
          c = @env.cursor((T2o.new pis[i2], (T2o.new cs.last.data.not_nil!).o).tup)
          return unless c.next && ((T2o.new c.data.not_nil!).t == pis[i2])
          cs << c
        end
        c2 = cs[i2]

        until (T2o.new c2.data.not_nil!).o >= (T2o.new c1.data.not_nil!).o
          return unless c2.next && ((T2o.new c2.data.not_nil!).t == pis[i2])
        end
        if ((T2o.new c2.data.not_nil!).o == (T2o.new c1.data.not_nil!).o)
          i1 = (i1 + 1) % present.size
          i2 = (i2 + 1) % present.size
        else
          until (T2o.new cs.first.data.not_nil!).o >= (T2o.new cs[i2].data.not_nil!).o
            return unless cs.first.next && ((T2o.new cs.first.data.not_nil!).t == pis.first)
          end
          i1 = 0
          i2 = 1
        end
      end
    end

    def find(present : Array(Bytes), absent : Array(Bytes) = [] of Bytes, limit : UInt32 = UInt32::MAX, from : Bytes? = nil)
      r = [] of Bytes
      find(present, absent, from) do |o|
        break if r.size == limit
        r << o
      end
      r
    end
  end
end
