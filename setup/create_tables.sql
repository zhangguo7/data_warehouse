CREATE TABLE `dimension_date` (
`dateKey` int(8) NOT NULL COMMENT '日期键',
`dateFullDate` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '完整日期',
`dateDayOfWeek` varchar(10) NOT NULL COMMENT '星期几',
`dateYear` int(4) NOT NULL COMMENT '年份',
`dateQuarter` int(1) NOT NULL COMMENT '季度',
`dateMonth` int(2) NOT NULL COMMENT '月份',
`dateDay` int(2) NOT NULL COMMENT '天',
`dateIsWeekday` varchar(10) NOT NULL COMMENT '是否工作日',
PRIMARY KEY (`dateKey`) ,
UNIQUE INDEX `日期` (`dateKey` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '日期类型表';

CREATE TABLE `fact_crawl` (
`crawlId` int(10) NOT NULL AUTO_INCREMENT COMMENT '抓取样本ID',
`divisionKey` int(5) NOT NULL COMMENT '行政区划键',
`crawlTimeKey` int(11) NOT NULL COMMENT '抓取时间键',
`crawlCreateDateKey` int(8) NOT NULL COMMENT '公司注册日期键',
`typeKey` int(2) NOT NULL COMMENT '公司类型键',
`statusKey` int(2) NOT NULL COMMENT '注册状态键',
`crawlDateKey` int(8) NOT NULL COMMENT '抓取日期',
`crawlCreditCode` varchar(50) NULL COMMENT '工商注册号',
`crawlLegalRepresentative` varchar(100) NULL COMMENT '法人代表',
`crawlCompanyName` varchar(255) NULL COMMENT '抓取样本公司名',
`crawlCompanyAddress` varchar(255) NULL COMMENT '抓取样本公司地址',
`crawlLongitude` numeric(9,6) NULL COMMENT '抓取样本经度',
`crawlLatitude` numeric(9,6) NULL COMMENT '抓取样本纬度',
`crawlTel` varchar(50) NULL COMMENT '注册电话',
`crawlBusinessScope` varchar(500) NULL COMMENT '经营范围',
`crawlRegistrationAuthority` varchar(100) NULL COMMENT '注册工商局',
`crawlRegisteredCapital` varchar(100) NULL COMMENT '注册资本',
`crawlGuid` varchar(255) NULL,
PRIMARY KEY (`crawlId`) ,
UNIQUE INDEX `抓取样本ID` (`crawlId` ASC) USING BTREE,
INDEX `行政区划` (`divisionKey` ASC) USING BTREE,
INDEX `抓取时间` (`crawlTimeKey` ASC) USING BTREE,
INDEX `抓取日期` (`crawlDateKey` ASC) USING BTREE,
INDEX `公司类型` (`typeKey` ASC) USING BTREE,
INDEX `注册状态` (`statusKey` ASC) USING BTREE,
INDEX `公司注册日期` (`crawlCreateDateKey` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '抓取事实表';

CREATE TABLE `dimension_division` (
`divisionKey` int(5) NOT NULL COMMENT '行政区划键(divisionCountyNo)',
`divisionProv` varchar(255) NOT NULL COMMENT '省份',
`divisionCity` varchar(255) NOT NULL COMMENT '城市',
`divisionDistrict` varchar(255) NOT NULL COMMENT '区县',
`divisionProvNo` int(2) NOT NULL COMMENT '省编码',
`divisionCityNo` int(4) NOT NULL COMMENT '城市编码',
`divisionRegion` varchar(0) NULL,
PRIMARY KEY (`divisionKey`) ,
UNIQUE INDEX `行政区划` (`divisionKey` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '行政区划维度表';

CREATE TABLE `dimension_time` (
`timeKey` int(11) NOT NULL COMMENT '时间键',
`timePeriod` varchar(2) NOT NULL COMMENT '时段',
`timeFullTime` time NOT NULL COMMENT '完整时间',
`timeHour` int(2) NOT NULL COMMENT '小时',
`timeMinute` int(2) NOT NULL COMMENT '分钟',
`timeSecond` int(2) NOT NULL COMMENT '秒',
PRIMARY KEY (`timeKey`) ,
UNIQUE INDEX `时间` (`timeKey` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '时间维度表';

CREATE TABLE `dimension_company_type` (
`typeKey` int(5) NOT NULL AUTO_INCREMENT COMMENT '公司类型键',
`typeFullType` varchar(255) NOT NULL COMMENT '完整的公司类型',
`typeIsIndividual` varchar(5) NOT NULL COMMENT '是否个体',
`typeIsCompany` varchar(5) NOT NULL COMMENT '是否是公司',
`typesIndividual2Company` varchar(5) NULL,
PRIMARY KEY (`typeKey`) 
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '公司类型维度表';

CREATE TABLE `dimension_registry_status` (
`statusKey` int(2) NOT NULL AUTO_INCREMENT COMMENT '注册状态键',
`statusIsNormal` varchar(20) NOT NULL COMMENT '状态是否正常',
`statusFullStatus` varchar(20) NOT NULL COMMENT '完整的注册状态',
PRIMARY KEY (`statusKey`) 
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '注册状态维度表';

CREATE TABLE `match_outcome` (
`matchedId` int(10) NOT NULL AUTO_INCREMENT,
`crawlId` int(10) NOT NULL COMMENT '抓取样本ID',
`drawId` int(10) NOT NULL COMMENT '绘图样本ID',
PRIMARY KEY (`matchedId`) ,
UNIQUE INDEX `匹配ID` (`matchedId` ASC) USING BTREE,
UNIQUE INDEX `绘图样本ID` (`drawId` ASC) USING BTREE,
UNIQUE INDEX `抓取样本ID` (`crawlId` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;

CREATE TABLE `fact_draw` (
`drawId` int(10) NOT NULL AUTO_INCREMENT COMMENT '绘图样本ID',
`drawGuid` varchar(50) NULL,
`marketGuid` varchar(50) NULL,
`drawZoneGuid` varchar(50) NULL,
`divisionKey` int(10) NOT NULL COMMENT '行政区划键',
`receiveDateKey` int(8) NOT NULL COMMENT '接收样本日期键',
`receiveTimeKey` int(6) NOT NULL COMMENT '接收样本时间键',
`inputDateKey` int(8) NOT NULL COMMENT '录入样本日期键',
`inputTimeKey` int(6) NOT NULL COMMENT '录入样本时间键',
`drawMateAddress` varchar(255) NULL COMMENT '工商局标准地址',
`drawDoorPlate` varchar(255) NULL COMMENT '门牌号',
`drawSelfNum` varchar(100) NULL COMMENT '自编号',
`drawCompanyName` varchar(255) NULL COMMENT '绘图样本公司名',
`drawCompanyAddress` varchar(255) NULL COMMENT '绘图商户地址',
`drawTel` varchar(255) NULL COMMENT '绘图商户电话',
`drawLongitude` numeric(9,6) NOT NULL COMMENT '绘图样本经度',
`drawLatitude` numeric(9,6) NOT NULL COMMENT '绘图样本纬度',
`drawPhotoCount` tinyint(2) NOT NULL COMMENT '绘图拍照数量',
`drawShopCount` tinyint(2) NOT NULL COMMENT '店铺数量',
`drawDecorate` varchar(10) NOT NULL COMMENT '装修情况',
`drawHagLicence` varchar(4) NULL COMMENT '有无悬挂营业执照',
`drawIndustryName_1` varchar(50) NULL COMMENT '一级行业名称',
`drawIndustryName_2` varchar(50) NULL COMMENT '二级行业名称',
`drawIndustryNo_1` varchar(10) NULL COMMENT '一级行业编号',
`drawIndustryNo_2` varchar(10) NULL COMMENT '二级行业编号',
`drawSublease` varchar(255) NULL COMMENT '转租',
`drawEmpty` varchar(255) NULL COMMENT '空置',
`drawRecruit` varchar(255) NULL COMMENT '正在招聘',
`drawRenovation` varchar(255) NULL COMMENT '正在装修',
`drawWarehouse` varchar(255) NULL COMMENT '商铺改为仓库',
PRIMARY KEY (`drawId`) ,
UNIQUE INDEX `绘图样本ID` (`drawId` ASC) USING BTREE,
INDEX `接收样本日期` (`receiveDateKey` ASC) USING BTREE,
INDEX `行政区划` (`divisionKey` ASC) USING BTREE,
INDEX `接收样本时间` (`receiveTimeKey` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '绘图事实表';

CREATE TABLE `fact_market` (
`marketId` int(10) NOT NULL COMMENT '市场(商场、街道)键',
`marketGuid` varchar(50) NULL COMMENT '源表的grandParentId',
`marketName` varchar(100) NOT NULL COMMENT '市场（商场、街道）名称',
`marketZoneGuid` varchar(11) NULL,
`marketZoneName` varchar(100) NOT NULL COMMENT '所在商圈的名称',
`divisionKey` int(10) NULL,
`marketTypeName` char(10) NOT NULL COMMENT '市场（商场、街道）类型',
`marketType` int(11) NULL COMMENT '1=街道，2=商场，3=市场',
`marketArea` float(10,0) NULL,
`marketBankOutletsNum` tinyint(2) NOT NULL,
`marketATMNum` tinyint(2) NULL,
`marketBusStationNum` tinyint(2) NOT NULL,
`marketBusStopNum` tinyint(2) NULL,
`marketTrainStationNum` tinyint(2) NULL,
`marketBusLineNum` tinyint(2) NULL,
`marketMetroStationNum` tinyint(2) NOT NULL,
`marketRestaurantNum` tinyint(2) NULL,
`marketHotelNum` tinyint(2) NULL,
`marketResidenceNum` tinyint(2) NULL,
`marketOfficeBuildingNum` tinyint(2) NULL,
`marketResidenceRent` int(6) NULL,
`marketOfficeBuildingRent` int(6) NULL,
`marketIndustry_1_1` varchar(50) NULL,
`marketIndustryNo_1_1` varchar(0) NULL,
`marketIndustry_1_2` varchar(255) NULL,
`marketIndustry_1_3` varchar(255) NULL,
`marketIndustry_2_1` varchar(255) NULL,
`marketIndustry_2_2` varchar(255) NULL,
`marketIndustry_2_3` varchar(255) NULL,
`marketIndustryNo_1_2` varchar(255) NULL,
`marketIndustryNo_1_3` varchar(255) NULL,
`marketIndustryNo_2_1` varchar(255) NULL,
`marketIndustryNo_2_2` varchar(255) NULL,
`marketIndustryNo_2_3` varchar(0) NULL,
PRIMARY KEY (`marketId`) ,
UNIQUE INDEX `市场(商场、街道)` (`marketId` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '市场（商场、街道）维度表';

CREATE TABLE `table_update` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`updateTime` time NOT NULL,
`updateDate` date NOT NULL,
`tableName` varchar(0) NOT NULL,
PRIMARY KEY (`id`) 
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '表更新记录';

CREATE TABLE `fact_crawl_tmp` (
`crawlId` int(10) NOT NULL AUTO_INCREMENT COMMENT '抓取样本ID',
`divisionKey` int(5) NOT NULL COMMENT '行政区划键',
`crawlTimeKey` int(11) NOT NULL COMMENT '抓取时间键',
`crawlCreateDateKey` int(8) NOT NULL COMMENT '公司注册日期键',
`typeKey` int(2) NOT NULL COMMENT '公司类型键',
`statusKey` int(2) NOT NULL COMMENT '注册状态键',
`crawlDateKey` int(8) NOT NULL COMMENT '抓取日期',
`crawlCreditCode` char(18) NULL COMMENT '工商注册号',
`crawlLegalRepresentative` varchar(100) NULL COMMENT '法人代表',
`crawlCompanyName` varchar(255) NULL COMMENT '抓取样本公司名',
`crawlCompanyAddress` varchar(255) NULL COMMENT '抓取样本公司地址',
`crawlTel` varchar(50) NULL,
`crawlBusinessScope` varchar(100) NULL,
`crawlRegistrationAuthority` varchar(100) NULL,
PRIMARY KEY (`crawlId`) ,
UNIQUE INDEX `抓取样本ID` (`crawlId` ASC) USING BTREE,
INDEX `行政区划` (`divisionKey` ASC) USING BTREE,
INDEX `抓取时间` (`crawlTimeKey` ASC) USING BTREE,
INDEX `抓取日期` (`crawlDateKey` ASC) USING BTREE,
INDEX `公司类型` (`typeKey` ASC) USING BTREE,
INDEX `注册状态` (`statusKey` ASC) USING BTREE,
INDEX `公司注册日期` (`crawlCreateDateKey` ASC) USING BTREE
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci
COMMENT = '抓取事实表';

CREATE TABLE `fact_macro_detail` (
`detailId` int(10) NOT NULL AUTO_INCREMENT,
`marketGuid` varchar(50) NULL COMMENT '源数据中的grandParentId',
`detailtagName` varchar(50) NULL COMMENT '源表中的tagName',
`detailShopName` varchar(100) NULL,
`divisionKey` int(10) NULL,
`detailLatitude` decimal(9,6) NULL,
`detailLongitude` decimal(9,6) NULL,
`detailAddress` varchar(255) NULL COMMENT '源表中的address',
`detailBusLine` varchar(255) NULL COMMENT '源表中的address(公交站点的address)',
`detailStreetId` varchar(50) NULL COMMENT '百度街道全景id',
`detailTelephone` varchar(255) NULL,
`detailUid` varchar(50) NULL COMMENT '获取详情需要的',
`detailDistance` int(10) NULL COMMENT '样本点到商圈中心的距离',
`detailBDType` varchar(255) NULL,
`detailBDTag` varchar(255) NULL,
`detailPrice` decimal(10,2) NULL,
`detailShopHours` varchar(255) NULL,
`detailOverallRating` varchar(255) NULL,
`detailTasteRating` varchar(255) NULL,
`detailServiceRating` varchar(255) NULL,
`detailEnvironmentRating` varchar(255) NULL,
`detailFacilityRating` varchar(255) NULL,
`detailHygieneRating` varchar(255) NULL,
`detailTechnologyRating` varchar(255) NULL,
`detailImageNum` varchar(255) NULL,
`detailGrouponNum` int(6) NULL,
`detailDiscountNum` int(6) NULL,
`detailCommentNum` int(6) NULL,
`detailFavoriteNum` int(6) NULL,
`detailCheckinNum` int(6) NULL,
`detailAtmosphere` text NULL,
`detailFeaturedService` text NULL,
`detailRecommendation` text NULL,
`detailDescription` text NULL,
`detailReviewKeyword` text NULL,
`detailCategory` text NULL,
`detailInnerFacility` text NULL,
`detailHotelFacility` text NULL,
`detailPaymentType` text NULL,
`detailAlias` text NULL,
`detailBrand` text NULL,
`detailHotelService` text NULL COMMENT '酒店服务',
`createDateKey` int NULL,
PRIMARY KEY (`detailId`) 
);

