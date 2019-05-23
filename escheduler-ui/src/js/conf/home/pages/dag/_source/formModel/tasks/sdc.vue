<template>
  <div class="sdc-model">
    <m-list-box>
      <div slot="text">{{$t('Custom Parameters')}}</div>
      <div slot="content">
        <m-local-params
          ref="refLocalParams"
          @on-udpData="_onUdpData"
          :udp-list="localParams">
        </m-local-params>
      </div>
    </m-list-box>
    <m-list-box>
      <div slot="text">{{$t('Source Stage')}}</div>
      <div slot="content">
        <m-sdc-stage :stage-list="sourceStageList" :stage-with-config="sourceStageConfig" @on-stageChange="_onSourceStageChange">
        </m-sdc-stage>
      </div>
    </m-list-box>
    <m-list-box>
      <div slot="text">{{$t('Processor Stage')}}</div>
      <div slot="content">
        <i class="iconfont" :class="_isDetails" data-toggle="tooltip" :title="$t('Add')">&#xe636;</i>
      </div>
    </m-list-box>
    <m-list-box>
      <div slot="text">{{$t('Target Stage')}}</div>
      <div slot="content">
        <m-sdc-stage :stage-list="targetStageList" :stage-with-config="targetStageConfig" @on-stageChange="_onTargetStageChange">
        </m-sdc-stage>
      </div>
    </m-list-box>
  </div>
</template>
<script>
  import _ from 'lodash'
  import i18n from '@/module/i18n'
  import mListBox from './_source/listBox'
  import mLocalParams from './_source/localParams'
  import mSdcStage from './_source/sdcStage'
  import disabledState from '@/module/mixin/disabledState'

  export default {
    name: 'sdc',
    data () {
      return {
        // Custom parameter
        localParams: [],
        // sdc stage list
        sourceStageList: [],
        sourceStageConfig: null,
        processorStageList: [],
        targetStageList: [],
        targetStageConfig: null
      }
    },
    mixins: [disabledState],
    props: {
      backfillItem: Object,
      createNodeId: Number
    },
    methods: {
      /**
       * return Custom parameter
       */
      _onUdpData (a) {
        this.localParams = a
      },
      _onSourceStageChange (a) {
        this.sourceStageConfig = a
      },
      _onTargetStageChange (a) {
        this.targetStageConfig = a
      },
      /**
       * verification
       */
      _verification () {
        // localParams Subcomponent verification
        if (!this.$refs.refLocalParams._verifProp()) {
          return false
        }
        // storage

        this.$emit('on-params', {
          localParams: this.localParams,
          sourceStageConfig: this.sourceStageConfig,
          targetStageConfig: this.targetStageConfig
        })
        return true
      },
      /**
       * set stage list
       */
      _setStageList(stageList) {
        this.sourceStageList = _.filter(stageList, d => d.type === 'SOURCE')
        this.processorStageList = _.filter(stageList, d => d.type === 'PROCESSOR')
        this.targetStageList = _.filter(stageList, d => d.type === 'TARGET')
      }
    },
    watch: {
    },
    created () {
      let o = this.backfillItem

      // Non-null objects represent backfill
      if (!_.isEmpty(o)) {
        // backfill
        this.localParams = o.params.localParams || []
        this.sourceStageConfig = o.params.sourceStageConfig
        this.targetStageConfig = o.params.targetStageConfig
      }
      if (!_.some(this.store.state.dag.tasks, { id: this.createNodeId }) &&
        this.router.history.current.name !== 'definition-create') {
        //this._getReceiver()
      }

      // load stage lists
      let stateSdcStageList = this.store.state.sdc.stageListAll || []
      if (stateSdcStageList.length) {
        this._setStageList(stateSdcStageList)
      } else {
        this.store.dispatch('sdc/getAllStages').then(res => {
          this.$nextTick(() => {
            this._setStageList(res)
          })
        })
      }
    },
    mounted () {
    },
    destroyed () {
    },
    computed: {},
    components: { mListBox, mLocalParams, mSdcStage }
  }
</script>
