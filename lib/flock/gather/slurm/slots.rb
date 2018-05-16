require 'colorize'
require 'yaml'
require 'flock/gather/comms'
require 'sparkr'

#NodeName=gpu01 Arch=x86_64 CoresPerSocket=8 CPUAlloc=3 CPUErr=0 CPUTot=16 CPULoad=0.02 AvailableFeatures=(null) ActiveFeatures=(null) Gres=gpu:4 NodeAddr=gpu01 NodeHostName=gpu01 Version=17.02 OS=Linux RealMemory=512000 AllocMem=28500 FreeMem=510505 Sockets=2 Boards=1 State=MIXED ThreadsPerCore=1 TmpDisk=0 Weight=1 Owner=N/A MCS_label=N/A Partitions=gpu  BootTime=2018-04-24T17:18:32 SlurmdStartTime=2018-04-25T08:43:36 CfgTRES=cpu=16,mem=500G AllocTRES=cpu=3,mem=28500M CapWatts=n/a CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s

module Flock
  module Gather
    module Slurm
      module Slots
        class << self
          MATCHERS = [
            /^phi/,
            /^login/,
            /^viz/,
          ]
          def skip?(name)
            MATCHERS.any? do |m|
              m.match(name)
            end
          end

          def spark_cell(line1, line2, data, key)
            vals = []
            data.each do |a|
              vals << (a[key] > 0 ? Math.log(a[key]) : 0)
            end
            puts vals.inspect
            avg = (vals.reduce(:+) / vals.length)
            max = vals.max
            vals << 0
            vals << Math.log(data.first[:total])
            sparkline = Sparkr::Sparkline.new(vals)
            sparkline.format do |tick, count, index|
              if index >= vals.length - 2
                nil
              elsif count == max
                tick.red
              elsif count > avg
                tick.yellow
              else
                tick.green
              end
            end

            line1.to_s.cyan << "\n" <<
              line2.to_s.cyan << ' ' <<
              sparkline.to_s
          end

          def run
            total = 0
            alloc = 0
            load = 0.0

            IO.popen(['cat','/tmp/sacct-data/show-node.txt']) do |io|
              #IO.popen(['scontrol','-o','show','node']) do |io|
              while !io.eof? do
                line = io.readline
                vals = line.split(' ')
                vals.each do |v|
                  key, val = v.split('=')
                  break if key == 'NodeName' && skip?(val)
                  if key == 'CPUTot'
                    total += val.to_i
                  elsif key == 'CPUAlloc'
                    alloc += val.to_i
                  elsif key == 'CPULoad'
                    load += val.to_f
                  end
                end
              end
            end

            load_avg = load / total
            puts "#{alloc}/#{total} (#{sprintf('%.2f%', (alloc * 100.0) / total)}) [#{sprintf('%.2f',load_avg)}]"

            # read last value, increment sparkline
            hist = YAML.load_file('slot-usage-history.yml') rescue []
            hist.pop
            hist.unshift({
                           alloc: alloc,
                           total: total
                         })
            while hist.length < 12
              hist.push({
                          alloc: 0,
                          total: total
                        })
            end
            slot_utilization = "#{alloc}/#{total} [#{Time.now.strftime("%H:%M")}]"
            pct_utilization = "#{sprintf('%.1f%', (alloc * 100.0) / total)} utilized"
            slot_padding = ' ' * (hist.length + 1)
            pct_padding = ' ' * (slot_utilization.length - pct_utilization.length)
            Flock::Gather::Comms.
              new(Gather.endpoint).
              set('usage.capacity',
                   spark_cell(
                     slot_utilization + slot_padding,
                     pct_utilization + pct_padding,
                     hist,
                     :alloc
                   ),
                   Gather.endpoint_auth)
          end
        end
      end
    end
  end
end

Flock::Gather.register('slurm/slots', Flock::Gather::Slurm::Slots)
