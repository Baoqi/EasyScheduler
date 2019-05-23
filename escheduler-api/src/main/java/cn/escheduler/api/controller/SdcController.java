package cn.escheduler.api.controller;

import cn.escheduler.api.enums.Status;
import cn.escheduler.api.sdc.PluginManager;
import cn.escheduler.api.sdc.StageDisplayInfo;
import cn.escheduler.api.utils.Constants;
import cn.escheduler.api.utils.Result;
import cn.escheduler.dao.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.escheduler.api.enums.Status.SDC_LOAD_PLUGIN_ERROR;

/**
 * sdc controller
 */
@RestController
@RequestMapping("sdc")
public class SdcController extends  BaseController {
    private static final Logger logger = LoggerFactory.getLogger(SdcController.class);

    /**
     * get all stage list
     * @param loginUser
     * @return
     */
    @GetMapping(value = "/list-stages")
    @ResponseStatus(HttpStatus.OK)
    public Result listStages(@RequestAttribute(value = Constants.SESSION_USER) User loginUser) {
        try{
            Map<String, Object> result = new HashMap<>(5);
            List<StageDisplayInfo> stages = PluginManager.getInstance().getAllStages();
            result.put(Constants.DATA_LIST, stages);
            putMsg(result, Status.SUCCESS);

            return returnDataList(result);
        } catch (Exception e){
            logger.error(SDC_LOAD_PLUGIN_ERROR.getMsg(),e);
            return error(SDC_LOAD_PLUGIN_ERROR.getCode(),
                    SDC_LOAD_PLUGIN_ERROR.getMsg());
        }
    }
}
