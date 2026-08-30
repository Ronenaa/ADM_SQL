var powerbi;
(function (powerbi) {
    var visuals;
    (function (visuals) {
        var CircularGauge1458053128559;
        (function (CircularGauge1458053128559) {
            //object variable which we used in customized color and text through UI options
            CircularGauge1458053128559.progressIndicatorProps = {
                general: {
                    ActualFillColor: { objectName: 'general', propertyName: 'ActualFillColor' },
                    ComparisonFillColor: { objectName: 'general', propertyName: 'ComparisonFillColor' },
                },
                custom: {
                    show: { objectName: 'custom', propertyName: 'show' },
                    ringWidth: { objectName: 'custom', propertyName: 'ringWidth' }
                },
                labels: {
                    color: { objectName: 'labels', propertyName: 'color' },
                    labelPrecision: { objectName: 'labels', propertyName: 'labelPrecision' },
                    fontSize: { objectName: 'labels', propertyName: 'fontSize' },
                },
            };
            //Visual
            var CircularGauge = (function () {
                function CircularGauge() {
                }
                CircularGauge.prototype.getDefaultFormatSettings = function () {
                    return {
                        showTitle: true,
                        textSize: null,
                        labelSettings: this.getDefaultLabelSettings(true, 'black', 0),
                        wordWrap: false,
                    };
                };
                CircularGauge.prototype.getDefaultLabelSettings = function (show, labelColor, labelPrecision) {
                    var defaultLabelPrecision = 0;
                    var defaultLabelColor = "#777777";
                    if (show === void 0) {
                        show = false;
                    }
                    var fontSize = 9;
                    return {
                        show: show,
                        precision: labelPrecision || defaultLabelPrecision,
                        labelColor: labelColor || defaultLabelColor,
                        fontSize: fontSize,
                    };
                };
                CircularGauge.getDefaultData = function () {
                    return {
                        actual: 0,
                        target: 100,
                        ringWidth: 20,
                        actualColor: '#374649',
                        targetColor: '#01B8AA',
                        isPie: true,
                        toolTipInfo: [],
                        actualFormat: '',
                        targetFormat: '',
                    };
                };
                //One time setup
                //First time it will be called and made the structure of your visual
                CircularGauge.prototype.init = function (options) {
                    this.root = d3.select(options.element.get(0));
                    this.svg = this.root.append('svg').style('overflow', 'visible');
                    this.container = this.svg.append('g');
                    this.group = this.container.append('g');
                    this.group.append('path').attr('id', 'a123');
                    this.groupInner = this.container.append('g');
                    this.groupInner.append('path').attr('id', 'a1234');
                    this.groupInner.append('text');
                    this.groupInner.append('line').attr('id', 'line1');
                    this.groupInner.append('line').attr('id', 'line2');
                };
                //Convert the dataview into its view model
                //All the variable will be populated with the value we have passed
                CircularGauge.converter = function (dataView) {
                    var data = CircularGauge.getDefaultData();
                    if (dataView && dataView.categorical) {
                        if (dataView.metadata && dataView.table.rows[0].length == 2) {
                            if (dataView.metadata.columns[0].roles['ActualValue']) {
                                data.actual = dataView.table.rows[0][0];
                                data.target = dataView.table.rows[0][1];
                                data.actualFormat = ((dataView.metadata.columns[0].format === '\\$#,0;(\\$#,0);\\$#,0') ? '$' : '');
                                data.targetFormat = ((dataView.metadata.columns[1].format === '\\$#,0;(\\$#,0);\\$#,0') ? '$' : '');
                            }
                            else {
                                data.actual = dataView.table.rows[0][1];
                                data.target = dataView.table.rows[0][0];
                                data.actualFormat = ((dataView.metadata.columns[1].format === '\\$#,0;(\\$#,0);\\$#,0') ? '$' : '');
                                data.targetFormat = ((dataView.metadata.columns[0].format === '\\$#,0;(\\$#,0);\\$#,0') ? '$' : '');
                            }
                        }
                        else if (dataView.metadata && dataView.table.rows[0].length == 1) {
                            if (dataView.metadata.columns[0].roles['ActualValue']) {
                                data.actual = dataView.table.rows[0][0];
                                data.target = data.actual;
                                data.actualFormat = ((dataView.metadata.columns[0].format === '\\$#,0;(\\$#,0);\\$#,0') ? '$' : '');
                            }
                            else {
                                data.actual = 0;
                                data.target = dataView.table.rows[0][0];
                                data.targetFormat = ((dataView.metadata.columns[0].format === '\\$#,0;(\\$#,0);\\$#,0') ? '$' : '');
                            }
                        }
                    }
                    return data; //Data object we are returning here to the update function
                };
                //Drawing the visual	   
                CircularGauge.prototype.update = function (options) {
                    var _this = this;
                    var dataView = this.dataView = options.dataViews[0];
                    var data2 = this.data = CircularGauge.converter(dataView); //calling Converter function			            
                    var data = data2.actual;
                    var max = data2.target;
                    var viewport = options.viewport;
                    var height = viewport.height;
                    var width = viewport.width;
                    this.svg.attr('width', width).attr('height', height);
                    this.cardFormatSetting = this.getDefaultFormatSettings();
                    var labelSettings = null;
                    var objects = null;
                    if (this.dataView && this.dataView.metadata) {
                        objects = this.dataView.metadata.objects;
                    }
                    if (objects) {
                        labelSettings = this.cardFormatSetting.labelSettings;
                        labelSettings.labelColor = powerbi.DataViewObjects.getFillColor(objects, visuals.cardProps.labels.color, labelSettings.labelColor);
                        labelSettings.precision = powerbi.DataViewObjects.getValue(objects, visuals.cardProps.labels.labelPrecision, labelSettings.precision);
                        // The precision can't go below 0
                        if (labelSettings.precision != null) {
                            this.cardFormatSetting.labelSettings.precision = (labelSettings.precision >= 0) ? labelSettings.precision : 0;
                        }
                        this.data.actualColor = powerbi.DataViewObjects.getFillColor(objects, CircularGauge1458053128559.progressIndicatorProps.general.ActualFillColor, this.data.actualColor);
                        this.data.targetColor = powerbi.DataViewObjects.getFillColor(objects, CircularGauge1458053128559.progressIndicatorProps.general.ComparisonFillColor, this.data.targetColor);
                        this.data.isPie = powerbi.DataViewObjects.getValue(objects, CircularGauge1458053128559.progressIndicatorProps.custom.show, this.data.isPie);
                        this.data.ringWidth = powerbi.DataViewObjects.getValue(objects, CircularGauge1458053128559.progressIndicatorProps.custom.ringWidth, this.data.ringWidth);
                        this.cardFormatSetting.labelSettings.fontSize = powerbi.DataViewObjects.getValue(objects, CircularGauge1458053128559.progressIndicatorProps.labels.fontSize, labelSettings.fontSize);
                    }
                    var percentCompleted = (data / max);
                    percentCompleted = isNaN(percentCompleted) || !isFinite(percentCompleted) ? 0 : (percentCompleted > 1) ? 1 : ((percentCompleted < 0) ? 0 : percentCompleted);
                    this.cardFormatSetting.labelSettings.precision = this.cardFormatSetting.labelSettings.precision < 4 ? this.cardFormatSetting.labelSettings.precision : 4;
                    var percentage = (percentCompleted * 100).toFixed(this.cardFormatSetting.labelSettings.precision);
                    var fontSize = this.cardFormatSetting.labelSettings.fontSize;
                    var textProperties = {
                        text: percentage + '%',
                        fontFamily: "sans-serif",
                        fontSize: ((4 / 3) * fontSize) + 'px'
                    };
                    var textWidth = powerbi.TextMeasurementService.measureSvgTextWidth(textProperties);
                    var textHeight = powerbi.TextMeasurementService.measureSvgTextHeight(textProperties);
                    var outerRadius = ((((width / 2) - (textWidth + 17)) < ((height / 2) - (textHeight)) ? ((width / 2) - (textWidth + 17)) : ((height / 2) - (textHeight))));
                    outerRadius = outerRadius - (outerRadius * 0.1);
                    var innerRadius;
                    this.data.ringWidth = this.data.ringWidth < 15 ? 15 : this.data.ringWidth;
                    innerRadius = outerRadius - this.data.ringWidth;
                    var arc, arc1;
                    if (innerRadius > 15) {
                        if (!this.data.isPie) {
                            innerRadius = 0;
                        }
                        arc = d3.svg.arc().innerRadius(innerRadius).outerRadius(outerRadius).startAngle(0).endAngle(2 * Math.PI);
                        arc1 = d3.svg.arc().innerRadius(innerRadius).outerRadius(outerRadius).startAngle(0).endAngle(2 * Math.PI * percentCompleted);
                        this.group.select('#a123').attr('d', arc).attr('fill', this.data.actualColor);
                        this.groupInner.select('#a1234').attr('d', arc1).attr('fill', this.data.targetColor);
                        var c = arc1.centroid(2 * Math.PI), x = c[0], y = c[1], 
                        // pythagorean theorem for hypotenuse
                        h = Math.sqrt(x * x + y * y);
                        var y1;
                        if (percentCompleted > 0.5)
                            y1 = (((y / h) * outerRadius * 1.1) + (textHeight / 3));
                        else
                            y1 = ((y / h) * outerRadius * 1.1) + (textHeight / 3);
                        this.groupInner.select("text").attr('x', ((x / h) * outerRadius * 1.1) + 17).attr('y', y1).attr("text-anchor", "start").attr('font-size', fontSize + 'pt').text(percentage + "%").attr('fill', this.cardFormatSetting.labelSettings.labelColor);
                        this.groupInner.select("line#line1").attr("x1", (x / h) * outerRadius * 1.02).attr("y1", (y / h) * outerRadius * 1.02).attr("x2", ((x / h) * outerRadius * 1.1)).attr("y2", (y / h) * outerRadius * 1.1).attr('style', "stroke:#DDDDDD;stroke-width:1");
                        this.groupInner.select("line#line2").attr("x1", (x / h) * outerRadius * 1.1).attr("y1", (y / h) * outerRadius * 1.1).attr("x2", ((x / h) * outerRadius * 1.1) + 15).attr("y2", (y / h) * outerRadius * 1.1).attr('style', "stroke:#DDDDDD;stroke-width:1");
                        if (percentCompleted < 0.10 || percentCompleted > 0.90) {
                            this.groupInner.select("text").attr('x', ((x / h) * outerRadius * 1.1) + 39);
                            this.groupInner.select("line#line2").attr("x2", ((x / h) * outerRadius * 1.1) + 35);
                        }
                    }
                    else {
                        outerRadius = (width / 2) < (height / 2) ? width / 2 : height / 2;
                        outerRadius = outerRadius - (outerRadius * 0.1);
                        var innerRadius;
                        if (this.data.isPie) {
                            this.data.ringWidth = this.data.ringWidth < 15 ? 15 : this.data.ringWidth;
                            innerRadius = outerRadius - this.data.ringWidth;
                        }
                        else {
                            innerRadius = 0;
                        }
                        arc = d3.svg.arc().innerRadius(innerRadius).outerRadius(outerRadius).startAngle(0).endAngle(2 * Math.PI);
                        arc1 = d3.svg.arc().innerRadius(innerRadius).outerRadius(outerRadius).startAngle(0).endAngle(2 * Math.PI * percentCompleted);
                        this.groupInner.select("text").attr('font-size', 0 + 'pt');
                        this.groupInner.select("line#line1").attr('style', "stroke-width:0");
                        this.groupInner.select("line#line2").attr('style', "stroke-width:0");
                    }
                    this.group.select('#a123').attr('d', arc).attr('fill', this.data.actualColor);
                    this.groupInner.select('#a1234').attr('d', arc1).attr('fill', this.data.targetColor);
                    this.group.attr('transform', 'translate(' + ((width / 2)) + ',' + ((height / 2)) + ')');
                    this.groupInner.attr('transform', 'translate(' + ((width / 2)) + ',' + ((height / 2)) + ')');
                    this.data.toolTipInfo[1] = {
                        displayName: 'Actual',
                        value: this.data.actualFormat + this.data.actual
                    };
                    this.data.toolTipInfo[0] = {
                        displayName: 'Target',
                        value: this.data.targetFormat + this.data.target
                    };
                    this.data.toolTipInfo[2] = {
                        displayName: 'Percentage Remaining',
                        value: (100 - parseFloat(percentage)) + '%'
                    };
                    visuals.TooltipManager.addTooltip(this.container, function (tooltipEvent) { return _this.data.toolTipInfo; }, false); //Adding visual tips
                };
                // Make visual properties available in the property pane in Power BI
                // values which we can customized from property pane in Power BI                
                CircularGauge.prototype.enumerateObjectInstances = function (options) {
                    var enumeration = new visuals.ObjectEnumerationBuilder();
                    if (!this.data)
                        this.data = CircularGauge.getDefaultData();
                    if (!this.cardFormatSetting)
                        this.cardFormatSetting = this.getDefaultFormatSettings();
                    var formatSettings = this.cardFormatSetting;
                    switch (options.objectName) {
                        case 'general':
                            enumeration.pushInstance({
                                objectName: 'general',
                                displayName: 'General',
                                selector: null,
                                properties: {
                                    ActualFillColor: this.data.actualColor,
                                    ComparisonFillColor: this.data.targetColor,
                                }
                            });
                            break;
                        case 'custom':
                            enumeration.pushInstance({
                                objectName: "custom",
                                displayName: "Donut Chart",
                                selector: null,
                                properties: {
                                    show: this.data.isPie,
                                    ringWidth: this.data.ringWidth
                                }
                            });
                            break;
                        case 'labels':
                            var labelSettingOptions;
                            labelSettingOptions = {
                                enumeration: enumeration,
                                dataLabelsSettings: formatSettings.labelSettings,
                                show: true,
                                precision: true,
                                fontSize: true,
                            };
                            visuals.dataLabelUtils.enumerateDataLabels(labelSettingOptions);
                            break;
                    }
                    return enumeration.complete();
                };
                //Capabilities what this visualization can do
                CircularGauge.capabilities = {
                    dataRoles: [
                        {
                            name: "ActualValue",
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: "Actual Value",
                        },
                        {
                            name: "TargetValue",
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: "Target Value",
                        }
                    ],
                    objects: {
                        general: {
                            displayName: 'General',
                            properties: {
                                ActualFillColor: {
                                    displayName: "Target Color",
                                    type: { fill: { solid: { color: true } } }
                                },
                                ComparisonFillColor: {
                                    displayName: "Value Color",
                                    type: { fill: { solid: { color: true } } }
                                },
                            },
                        },
                        custom: {
                            displayName: "Donut Chart",
                            properties: {
                                show: {
                                    displayName: 'Visual',
                                    type: { bool: true }
                                },
                                ringWidth: {
                                    displayName: "Ring Width",
                                    type: { formatting: { fontSize: true } }
                                }
                            }
                        },
                        labels: {
                            displayName: 'Data label',
                            properties: {
                                color: {
                                    displayName: 'Color',
                                    type: { fill: { solid: { color: true } } }
                                },
                                labelPrecision: {
                                    displayName: 'Decimal Places',
                                    type: { numeric: true }
                                },
                                fontSize: {
                                    displayName: 'Text Size',
                                    type: { formatting: { fontSize: true } }
                                },
                            },
                        }
                    },
                    dataViewMappings: [{
                        conditions: [
                            { 'ActualValue': { max: 1 }, 'TargetValue': { max: 1 } },
                        ],
                        categorical: {
                            values: {
                                select: [
                                    { bind: { to: 'ActualValue' } },
                                    { bind: { to: 'TargetValue' } }
                                ]
                            },
                        },
                    }],
                    suppressDefaultTitle: false,
                };
                return CircularGauge;
            })();
            CircularGauge1458053128559.CircularGauge = CircularGauge;
        })(CircularGauge1458053128559 = visuals.CircularGauge1458053128559 || (visuals.CircularGauge1458053128559 = {}));
    })(visuals = powerbi.visuals || (powerbi.visuals = {}));
})(powerbi || (powerbi = {}));
var powerbi;
(function (powerbi) {
    var visuals;
    (function (visuals) {
        var plugins;
        (function (plugins) {
            plugins.CircularGauge1458053128559 = {
                name: 'CircularGauge1458053128559',
                class: 'CircularGauge1458053128559',
                capabilities: powerbi.visuals.CircularGauge1458053128559.CircularGauge.capabilities,
                custom: true,
                create: function (options) { return new powerbi.visuals.CircularGauge1458053128559.CircularGauge(options); },
                apiVersion: null
            };
        })(plugins = visuals.plugins || (visuals.plugins = {}));
    })(visuals = powerbi.visuals || (powerbi.visuals = {}));
})(powerbi || (powerbi = {}));
