/*
 *  Card With States By SQLBI
 *  v1.2.5
 *
 *  Change log:
 *   - Fixed a bug in the Update event
 *   - Removed the uppercase forced formatting of performance labels
 *   - Fixed component padding space when showing performance labels
 *
 *
 *  Contact mailto:support@okviz.com
 *  Support URL http://okviz.com/card-with-states/
 *
 *  Copyright (c) SQLBI.  All rights reserved. OkViz is a trademark of SQLBI Corp.
 *
 *  MIT License
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the ""Software""), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
///// <reference path="../_references.ts"/>
var powerbi;
(function (powerbi) {
    var visuals;
    (function (visuals) {
        var CardWithStatesBySQLBI1445315565700;
        (function (CardWithStatesBySQLBI1445315565700) {
            CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps = {
                cardTitle: {
                    show: { objectName: 'cardTitle', propertyName: 'show' },
                    color: { objectName: 'cardTitle', propertyName: 'color' },
                    text: { objectName: 'cardTitle', propertyName: 'text' },
                    fontSize: { objectName: 'cardTitle', propertyName: 'fontSize' },
                    wordWrap: { objectName: 'cardTitle', propertyName: 'wordWrap' },
                    topMargin: { objectName: 'cardTitle', propertyName: 'topMargin' },
                },
                labels: {
                    color: { objectName: 'labels', propertyName: 'color' },
                    labelPrecision: { objectName: 'labels', propertyName: 'labelPrecision' },
                    labelDisplayUnits: { objectName: 'labels', propertyName: 'labelDisplayUnits' },
                    fontSize: { objectName: 'labels', propertyName: 'fontSize' },
                },
                dataState1: {
                    color: { objectName: 'dataState1', propertyName: 'color' },
                    dataMin: { objectName: 'dataState1', propertyName: 'dataMin' },
                    dataMax: { objectName: 'dataState1', propertyName: 'dataMax' },
                    showLabel: { objectName: 'dataState1', propertyName: 'showLabel' },
                    label: { objectName: 'dataState1', propertyName: 'label' },
                },
                dataState2: {
                    color: { objectName: 'dataState2', propertyName: 'color' },
                    dataMin: { objectName: 'dataState2', propertyName: 'dataMin' },
                    dataMax: { objectName: 'dataState2', propertyName: 'dataMax' },
                    showLabel: { objectName: 'dataState2', propertyName: 'showLabel' },
                    label: { objectName: 'dataState2', propertyName: 'label' },
                },
                dataState3: {
                    color: { objectName: 'dataState3', propertyName: 'color' },
                    dataMin: { objectName: 'dataState3', propertyName: 'dataMin' },
                    dataMax: { objectName: 'dataState3', propertyName: 'dataMax' },
                    showLabel: { objectName: 'dataState3', propertyName: 'showLabel' },
                    label: { objectName: 'dataState3', propertyName: 'label' },
                }
            };
            var CardWithStatesBySQLBI = (function () {
                function CardWithStatesBySQLBI(options) {
                    this.displayUnitSystemType = powerbi.DisplayUnitSystemType.WholeUnits;
                    if (options) {
                        if (options.displayUnitSystemType != null)
                            this.displayUnitSystemType = options.displayUnitSystemType;
                    }
                }
                CardWithStatesBySQLBI.prototype.init = function (options) {
                    this.animationOptions = options.animation;
                    var element = options.element;
                    var svg = this.svg = d3.select(element.get(0)).append('svg');
                    this.graphicsContext = svg.append('g');
                    this.currentViewport = options.viewport;
                    this.hostServices = options.host;
                    this.style = options.style;
                    this.updateViewportProperties();
                    svg.attr('class', CardWithStatesBySQLBI.cardClassName);
                    this.labelContext = svg.append('g');
                };
                CardWithStatesBySQLBI.prototype.update = function (options) {
                    if (!options.dataViews || !options.dataViews[0])
                        return;
                    this.updateViewport(options.viewport);
                    //Default settings for reset to default
                    this.cardFormatSettings = this.getDefaultFormatSettings();
                    var dataView = options.dataViews[0];
                    var value;
                    var target;
                    var valueCol;
                    if (dataView && dataView.categorical && dataView.metadata && dataView.metadata.columns) {
                        var objects = dataView.metadata.objects;
                        if (objects) {
                            var labelSettings = this.cardFormatSettings.labelSettings;
                            labelSettings.labelColor = powerbi.DataViewObjects.getFillColor(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.labels.color, labelSettings.labelColor);
                            labelSettings.precision = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.labels.labelPrecision, labelSettings.precision);
                            // The precision can't go below 0 and beyond 9
                            if (labelSettings.precision != null) {
                                if (labelSettings.precision < 0)
                                    labelSettings.precision = 0;
                                else if (labelSettings.precision > 9)
                                    labelSettings.precision = 9;
                                this.cardFormatSettings.labelSettings.precision = labelSettings.precision;
                            }
                            labelSettings.displayUnits = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.labels.labelDisplayUnits, labelSettings.displayUnits);
                            labelSettings.fontSize = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.labels.fontSize, labelSettings.fontSize);
                            var titleSettings = this.cardFormatSettings.titleSettings;
                            titleSettings.show = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.cardTitle.show, titleSettings.show);
                            titleSettings.color = powerbi.DataViewObjects.getFillColor(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.cardTitle.color, titleSettings.color);
                            titleSettings.text = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.cardTitle.text, titleSettings.text);
                            titleSettings.fontSize = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.cardTitle.fontSize, titleSettings.fontSize);
                            titleSettings.wordWrap = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.cardTitle.wordWrap, titleSettings.wordWrap);
                            titleSettings.topMargin = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.cardTitle.topMargin, titleSettings.topMargin);
                            var dataState1 = this.cardFormatSettings.dataState1;
                            dataState1.color = powerbi.DataViewObjects.getFillColor(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState1.color, dataState1.color);
                            dataState1.dataMin = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState1.dataMin, dataState1.dataMin);
                            dataState1.dataMax = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState1.dataMax, dataState1.dataMax);
                            dataState1.label = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState1.label, dataState1.label);
                            dataState1.showLabel = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState1.showLabel, dataState1.showLabel);
                            var dataState2 = this.cardFormatSettings.dataState2;
                            dataState2.color = powerbi.DataViewObjects.getFillColor(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState2.color, dataState2.color);
                            dataState2.dataMin = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState2.dataMin, dataState2.dataMin);
                            dataState2.dataMax = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState2.dataMax, dataState2.dataMax);
                            dataState2.label = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState2.label, dataState2.label);
                            dataState2.showLabel = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState2.showLabel, dataState2.showLabel);
                            var dataState3 = this.cardFormatSettings.dataState3;
                            dataState3.color = powerbi.DataViewObjects.getFillColor(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState3.color, dataState3.color);
                            dataState3.dataMin = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState3.dataMin, dataState3.dataMin);
                            dataState3.dataMax = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState3.dataMax, dataState3.dataMax);
                            dataState3.label = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState3.label, dataState3.label);
                            dataState3.showLabel = powerbi.DataViewObjects.getValue(objects, CardWithStatesBySQLBI1445315565700.cardWithStatesBySQLBIProps.dataState3.showLabel, dataState3.showLabel);
                        }
                        if (dataView.categorical.categories) {
                            var categories = dataView.categorical.categories;
                            for (var i = 0; i < categories.length; i++) {
                                var col = categories[i].source;
                                if (categories[i].values) {
                                    for (var ii = 0; ii < categories[i].values.length; ii++) {
                                        var v = categories[i].values[ii];
                                        if (col && col.roles) {
                                            if (col.roles['Values']) {
                                                if (value && typeof value != 'string')
                                                    value += v;
                                                else
                                                    value = v;
                                                valueCol = col;
                                                if (typeof (this.cardFormatSettings.titleSettings.text) === 'undefined')
                                                    this.cardFormatSettings.titleSettings.text = col.displayName;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (dataView.categorical.values) {
                            var values = dataView.categorical.values;
                            for (var i = 0; i < values.length; i++) {
                                var col = values[i].source;
                                var v = values[i].values[0] || 0;
                                if (col && col.roles) {
                                    if (col.roles['Values']) {
                                        if (value)
                                            value += v;
                                        else
                                            value = v;
                                        valueCol = col;
                                        if (typeof (this.cardFormatSettings.titleSettings.text) === 'undefined')
                                            this.cardFormatSettings.titleSettings.text = col.displayName;
                                    }
                                    if (col.roles['TargetValue']) {
                                        target = v;
                                    }
                                    if (col.roles['State1Min'])
                                        this.cardFormatSettings.dataState1.dataMin = v;
                                    if (col.roles['State1Max'])
                                        this.cardFormatSettings.dataState1.dataMax = v;
                                    if (col.roles['State2Min'])
                                        this.cardFormatSettings.dataState2.dataMin = v;
                                    if (col.roles['State2Max'])
                                        this.cardFormatSettings.dataState2.dataMax = v;
                                    if (col.roles['State3Min'])
                                        this.cardFormatSettings.dataState3.dataMin = v;
                                    if (col.roles['State3Max'])
                                        this.cardFormatSettings.dataState3.dataMax = v;
                                }
                            }
                        }
                    }
                    var start = this.value;
                    if (value === undefined) {
                        if (start !== undefined)
                            this.clear();
                        return;
                    }
                    var labelSettings = this.cardFormatSettings.labelSettings;
                    var isDefaultDisplayUnit = (labelSettings.displayUnits === 0);
                    var formattedValue = value;
                    if (valueCol) {
                        var formatString = visuals.valueFormatter.getFormatString(valueCol, { objectName: 'general', propertyName: 'formatString' });
                        var formatter = visuals.valueFormatter.create({
                            format: formatString,
                            value: isDefaultDisplayUnit ? value : labelSettings.displayUnits,
                            precision: labelSettings.precision,
                            displayUnitSystemType: isDefaultDisplayUnit && labelSettings.precision === 0 ? this.displayUnitSystemType : powerbi.DisplayUnitSystemType.WholeUnits,
                            formatSingleValues: isDefaultDisplayUnit ? true : false,
                            allowFormatBeautification: true,
                            columnType: valueCol ? valueCol.type : undefined
                        });
                        powerbi.ValueType.fromPrimitiveTypeAndCategory(powerbi.PrimitiveType.Decimal);
                        formattedValue = formatter.format(value);
                    }
                    var labelStyles = CardWithStatesBySQLBI.DefaultStyle.label;
                    var labelFontSize = parseInt(jsCommon.PixelConverter.fromPoint(this.cardFormatSettings.titleSettings.fontSize));
                    var valueStyles = CardWithStatesBySQLBI.DefaultStyle.value;
                    var valueFontSize = parseInt(jsCommon.PixelConverter.fromPoint(labelSettings.fontSize));
                    if (start !== value)
                        value = formattedValue;
                    var performanceLabel = '';
                    var valueColor = labelSettings.labelColor;
                    if (target <= this.cardFormatSettings.dataState1.dataMax && target >= this.cardFormatSettings.dataState1.dataMin) {
                        valueColor = this.cardFormatSettings.dataState1.color;
                        if (this.cardFormatSettings.dataState1.showLabel)
                            performanceLabel = this.cardFormatSettings.dataState1.label;
                    }
                    else if (target <= this.cardFormatSettings.dataState2.dataMax && target >= this.cardFormatSettings.dataState2.dataMin) {
                        valueColor = this.cardFormatSettings.dataState2.color;
                        if (this.cardFormatSettings.dataState2.showLabel)
                            performanceLabel = this.cardFormatSettings.dataState2.label;
                    }
                    else if (target <= this.cardFormatSettings.dataState3.dataMax && target >= this.cardFormatSettings.dataState3.dataMin) {
                        valueColor = this.cardFormatSettings.dataState3.color;
                        if (this.cardFormatSettings.dataState3.showLabel)
                            performanceLabel = this.cardFormatSettings.dataState3.label;
                    }
                    var translateX = this.currentViewport.width / 2;
                    var translateY = (this.cardFormatSettings.titleSettings.wordWrap ? 2 : (this.currentViewport.height - labelFontSize - valueFontSize) / 2);
                    if (performanceLabel !== '' && !this.cardFormatSettings.titleSettings.wordWrap)
                        translateY -= labelFontSize;
                    var valueElement = this.graphicsContext.attr('transform', visuals.SVGUtil.translate(translateX, valueFontSize + translateY)).selectAll('text').data([value]);
                    valueElement.enter().append('text').attr('class', CardWithStatesBySQLBI.Value.class);
                    valueElement.text(function (d) { return d; }).style({
                        'font-size': valueFontSize + 'px',
                        'fill': valueColor,
                        'font-family': valueStyles.fontFamily,
                        'text-anchor': 'middle'
                    });
                    valueElement.call(visuals.AxisHelper.LabelLayoutStrategy.clip, this.currentViewport.width, powerbi.TextMeasurementService.svgEllipsis);
                    valueElement.exit().remove();
                    translateY = valueFontSize + labelFontSize + translateY + this.cardFormatSettings.titleSettings.topMargin;
                    this.labelContext.selectAll('.unit').remove();
                    if (this.cardFormatSettings.titleSettings.show) {
                        var labelElement = this.labelContext.append('text').classed('unit', true).attr('transform', 'translate(' + translateX + ',' + translateY + ')').text(this.cardFormatSettings.titleSettings.text).style({
                            'font-size': labelFontSize + 'px',
                            'fill': this.cardFormatSettings.titleSettings.color,
                            'text-anchor': 'middle'
                        });
                        if (this.cardFormatSettings.titleSettings.wordWrap) {
                            var labelElementNode = labelElement.node();
                            powerbi.TextMeasurementService.wordBreak(labelElementNode, this.currentViewport.width, this.currentViewport.height - translateY);
                            translateY += (labelElementNode.childNodes.length * (labelFontSize + 5)) + 10;
                        }
                        else {
                            labelElement.call(visuals.AxisHelper.LabelLayoutStrategy.clip, this.currentViewport.width, powerbi.TextMeasurementService.svgEllipsis);
                            translateY += labelFontSize + 10;
                        }
                    }
                    this.labelContext.selectAll('.perf').remove();
                    this.labelContext.selectAll('circle').remove();
                    if (performanceLabel !== '') {
                        var radius = 2;
                        var margin = 10;
                        translateX += (radius + (margin / 2));
                        var performanceElement = this.labelContext.append('text').classed('perf', true).attr('transform', 'translate(' + translateX + ',' + translateY + ')').text(performanceLabel).style({
                            'font-size': (labelFontSize - 4) + 'px',
                            'fill': '#a6a6a6',
                            'text-anchor': 'middle'
                        });
                        performanceElement.call(visuals.AxisHelper.LabelLayoutStrategy.clip, this.currentViewport.width, powerbi.TextMeasurementService.svgEllipsis);
                        var labelWidth = powerbi.TextMeasurementService.measureSvgTextWidth({
                            fontFamily: valueStyles.fontFamily,
                            fontSize: (labelFontSize - 4) + 'px',
                            text: performanceLabel
                        });
                        this.labelContext.append('circle').attr('cx', translateX - (labelWidth / 2) - margin).attr('cy', translateY - radius - 2).attr('r', radius * 2).attr('fill', valueColor);
                    }
                    if (!this.toolTip)
                        this.toolTip = this.graphicsContext.append("svg:title");
                    this.toolTip.text(value);
                    this.value = value;
                    this.target = target;
                };
                CardWithStatesBySQLBI.prototype.updateViewport = function (viewport) {
                    this.currentViewport = viewport;
                    this.updateViewportProperties();
                };
                CardWithStatesBySQLBI.prototype.updateViewportProperties = function () {
                    var viewport = this.currentViewport;
                    this.svg.attr('width', viewport.width).attr('height', viewport.height);
                };
                CardWithStatesBySQLBI.prototype.getAdjustedFontHeight = function (availableWidth, textToMeasure, seedFontHeight) {
                    var nodeSelection = this.svg.append('text').text(textToMeasure);
                    var adjustedFontHeight = this.getAdjustedFontHeightCore(nodeSelection, availableWidth, seedFontHeight, 0);
                    nodeSelection.remove();
                    return Math.min(adjustedFontHeight, CardWithStatesBySQLBI.DefaultStyle.card.maxFontSize);
                };
                CardWithStatesBySQLBI.prototype.getAdjustedFontHeightCore = function (nodeToMeasure, availableWidth, seedFontHeight, iteration) {
                    // Too many attempts - just return what we have so we don't sacrifice perf
                    if (iteration > 10)
                        return seedFontHeight;
                    nodeToMeasure.attr('font-size', seedFontHeight);
                    var candidateLength = powerbi.TextMeasurementService.measureSvgTextElementWidth(nodeToMeasure[0][0]);
                    if (candidateLength < availableWidth)
                        return seedFontHeight;
                    return this.getAdjustedFontHeightCore(nodeToMeasure, availableWidth, seedFontHeight * 0.9, iteration + 1);
                };
                CardWithStatesBySQLBI.prototype.clear = function (valueOnly) {
                    if (valueOnly === void 0) { valueOnly = false; }
                    this.svg.select(CardWithStatesBySQLBI.Value.selector).text('');
                    this.labelContext.selectAll('.perf').remove();
                    this.labelContext.selectAll('circle').remove();
                    if (!valueOnly)
                        this.svg.select(CardWithStatesBySQLBI.Label.selector).text('');
                };
                CardWithStatesBySQLBI.prototype.getDefaultFormatSettings = function () {
                    return {
                        titleSettings: {
                            show: true,
                            color: CardWithStatesBySQLBI.DefaultStyle.label.color,
                            text: undefined,
                            fontSize: CardWithStatesBySQLBI.DefaultStyle.label.fontSize,
                            wordWrap: false,
                            topMargin: 0
                        },
                        labelSettings: visuals.dataLabelUtils.getDefaultCardLabelSettings(CardWithStatesBySQLBI.DefaultStyle.value.color, CardWithStatesBySQLBI.DefaultStyle.label.color, CardWithStatesBySQLBI.DefaultStyle.value.fontSize),
                        dataState1: {
                            color: '#FD625E',
                            dataMin: -Infinity,
                            dataMax: 0,
                            showLabel: false,
                            label: 'Fail'
                        },
                        dataState2: {
                            color: '#F2C811',
                            dataMin: 0,
                            dataMax: 1,
                            showLabel: false,
                            label: 'Moderate'
                        },
                        dataState3: {
                            color: '#7DC172',
                            dataMin: 1,
                            dataMax: Infinity,
                            showLabel: false,
                            label: 'Great'
                        },
                    };
                };
                CardWithStatesBySQLBI.prototype.enumerateObjectInstances = function (options) {
                    if (!this.cardFormatSettings)
                        this.cardFormatSettings = this.getDefaultFormatSettings();
                    switch (options.objectName) {
                        case 'labels':
                            return [{
                                objectName: 'labels',
                                selector: null,
                                properties: {
                                    color: this.cardFormatSettings.labelSettings.labelColor,
                                    labelDisplayUnits: this.cardFormatSettings.labelSettings.displayUnits,
                                    labelPrecision: this.cardFormatSettings.labelSettings.precision,
                                    fontSize: this.cardFormatSettings.labelSettings.fontSize
                                },
                            }];
                        case 'cardTitle':
                            return [{
                                objectName: 'cardTitle',
                                selector: null,
                                properties: {
                                    show: this.cardFormatSettings.titleSettings.show,
                                    text: this.cardFormatSettings.titleSettings.text,
                                    wordWrap: this.cardFormatSettings.titleSettings.wordWrap,
                                    color: this.cardFormatSettings.titleSettings.color,
                                    fontSize: this.cardFormatSettings.titleSettings.fontSize,
                                    topMargin: this.cardFormatSettings.titleSettings.topMargin,
                                },
                            }];
                        case 'dataState1':
                            return [{
                                objectName: 'dataState1',
                                selector: null,
                                properties: {
                                    dataMin: this.cardFormatSettings.dataState1.dataMin,
                                    dataMax: this.cardFormatSettings.dataState1.dataMax,
                                    color: this.cardFormatSettings.dataState1.color,
                                    showLabel: this.cardFormatSettings.dataState1.showLabel,
                                    label: this.cardFormatSettings.dataState1.label
                                },
                            }];
                        case 'dataState2':
                            return [{
                                objectName: 'dataState2',
                                selector: null,
                                properties: {
                                    dataMin: this.cardFormatSettings.dataState2.dataMin,
                                    dataMax: this.cardFormatSettings.dataState2.dataMax,
                                    color: this.cardFormatSettings.dataState2.color,
                                    showLabel: this.cardFormatSettings.dataState2.showLabel,
                                    label: this.cardFormatSettings.dataState2.label
                                },
                            }];
                        case 'dataState3':
                            return [{
                                objectName: 'dataState3',
                                selector: null,
                                properties: {
                                    dataMin: this.cardFormatSettings.dataState3.dataMin,
                                    dataMax: this.cardFormatSettings.dataState3.dataMax,
                                    color: this.cardFormatSettings.dataState3.color,
                                    showLabel: this.cardFormatSettings.dataState3.showLabel,
                                    label: this.cardFormatSettings.dataState3.label
                                },
                            }];
                    }
                };
                CardWithStatesBySQLBI.cardClassName = 'cardWithStates';
                CardWithStatesBySQLBI.Label = {
                    class: 'label',
                    selector: '.label'
                };
                CardWithStatesBySQLBI.Value = {
                    class: 'value',
                    selector: '.value'
                };
                CardWithStatesBySQLBI.DefaultStyle = {
                    card: {
                        maxFontSize: 200
                    },
                    label: {
                        fontSize: 12,
                        color: '#a6a6a6',
                        height: 26
                    },
                    value: {
                        fontSize: 27,
                        color: '#333333',
                        fontFamily: 'wf_segoe-ui_Semibold'
                    }
                };
                //Capabilities
                CardWithStatesBySQLBI.capabilities = {
                    dataRoles: [
                        {
                            name: 'Values',
                            kind: powerbi.VisualDataRoleKind.GroupingOrMeasure,
                            displayName: 'Field'
                        },
                        {
                            name: 'TargetValue',
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: 'State Value',
                        },
                        {
                            name: 'State1Min',
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: 'State 1 Min',
                        },
                        {
                            name: 'State1Max',
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: 'State 1 Max',
                        },
                        {
                            name: 'State2Min',
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: 'State 2 Min',
                        },
                        {
                            name: 'State2Max',
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: 'State 2 Max',
                        },
                        {
                            name: 'State3Min',
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: 'State 3 Min',
                        },
                        {
                            name: 'State3Max',
                            kind: powerbi.VisualDataRoleKind.Measure,
                            displayName: 'State 3 Max',
                        }
                    ],
                    objects: {
                        general: {
                            properties: {
                                formatString: {
                                    type: { formatting: { formatString: true } },
                                },
                            },
                        },
                        labels: {
                            displayName: 'Data label',
                            properties: {
                                color: {
                                    displayName: 'Color',
                                    type: { fill: { solid: { color: true } } }
                                },
                                labelDisplayUnits: {
                                    displayName: 'Display units',
                                    type: { formatting: { labelDisplayUnits: true } }
                                },
                                labelPrecision: {
                                    displayName: 'Decimal points',
                                    type: { numeric: true }
                                },
                                fontSize: {
                                    displayName: 'Text size',
                                    type: { formatting: { fontSize: true } }
                                },
                            },
                        },
                        dataState1: {
                            displayName: 'State 1',
                            properties: {
                                color: {
                                    displayName: 'Color',
                                    type: { fill: { solid: { color: true } } }
                                },
                                dataMin: {
                                    displayName: 'From value',
                                    type: { numeric: true }
                                },
                                dataMax: {
                                    displayName: 'To value',
                                    type: { numeric: true }
                                },
                                showLabel: {
                                    displayName: 'Label',
                                    type: { bool: true }
                                },
                                label: {
                                    displayName: 'Label text',
                                    type: { text: true }
                                },
                            },
                        },
                        dataState2: {
                            displayName: 'State 2',
                            properties: {
                                color: {
                                    displayName: 'Color',
                                    type: { fill: { solid: { color: true } } }
                                },
                                dataMin: {
                                    displayName: 'From value',
                                    type: { numeric: true }
                                },
                                dataMax: {
                                    displayName: 'To value',
                                    type: { numeric: true }
                                },
                                showLabel: {
                                    displayName: 'Label',
                                    type: { bool: true }
                                },
                                label: {
                                    displayName: 'Label text',
                                    type: { text: true }
                                },
                            },
                        },
                        dataState3: {
                            displayName: 'State 3',
                            properties: {
                                color: {
                                    displayName: 'Color',
                                    type: { fill: { solid: { color: true } } }
                                },
                                dataMin: {
                                    displayName: 'From value',
                                    type: { numeric: true }
                                },
                                dataMax: {
                                    displayName: 'To value',
                                    type: { numeric: true }
                                },
                                showLabel: {
                                    displayName: 'Label',
                                    type: { bool: true }
                                },
                                label: {
                                    displayName: 'Label text',
                                    type: { text: true }
                                },
                            },
                        },
                        cardTitle: {
                            displayName: 'Category label',
                            properties: {
                                color: {
                                    displayName: 'Color',
                                    type: { fill: { solid: { color: true } } }
                                },
                                text: {
                                    displayName: 'Text',
                                    type: { text: true }
                                },
                                show: {
                                    displayName: 'Show',
                                    type: { bool: true }
                                },
                                wordWrap: {
                                    displayName: 'Word wrap',
                                    type: { bool: true }
                                },
                                fontSize: {
                                    displayName: 'Text size',
                                    type: { formatting: { fontSize: true } }
                                },
                                topMargin: {
                                    displayName: 'Margin top',
                                    type: { numeric: true }
                                },
                            },
                        }
                    },
                    dataViewMappings: [{
                        conditions: [
                            { 'Values': { max: 1 }, 'TargetValue': { max: 1 }, 'State1Min': { max: 1 }, 'State1Max': { max: 1 }, 'State2Min': { max: 1 }, 'State2Max': { max: 1 }, 'State3Min': { max: 1 }, 'State3Max': { max: 1 } }
                        ],
                        categorical: {
                            categories: {
                                for: { in: 'Values' },
                                dataReductionAlgorithm: { window: {} }
                            },
                            values: {
                                select: [
                                    { bind: { to: 'TargetValue' } },
                                    { bind: { to: 'State1Min' } },
                                    { bind: { to: 'State1Max' } },
                                    { bind: { to: 'State2Min' } },
                                    { bind: { to: 'State2Max' } },
                                    { bind: { to: 'State3Min' } },
                                    { bind: { to: 'State3Max' } },
                                ]
                            },
                        },
                    }],
                    suppressDefaultTitle: true,
                };
                return CardWithStatesBySQLBI;
            })();
            CardWithStatesBySQLBI1445315565700.CardWithStatesBySQLBI = CardWithStatesBySQLBI;
        })(CardWithStatesBySQLBI1445315565700 = visuals.CardWithStatesBySQLBI1445315565700 || (visuals.CardWithStatesBySQLBI1445315565700 = {}));
    })(visuals = powerbi.visuals || (powerbi.visuals = {}));
})(powerbi || (powerbi = {}));
var powerbi;
(function (powerbi) {
    var visuals;
    (function (visuals) {
        var plugins;
        (function (plugins) {
            plugins.CardWithStatesBySQLBI1445315565700 = {
                name: 'CardWithStatesBySQLBI1445315565700',
                class: 'CardWithStatesBySQLBI1445315565700',
                capabilities: powerbi.visuals.CardWithStatesBySQLBI1445315565700.CardWithStatesBySQLBI.capabilities,
                custom: true,
                create: function (options) { return new powerbi.visuals.CardWithStatesBySQLBI1445315565700.CardWithStatesBySQLBI(options); },
                apiVersion: null
            };
        })(plugins = visuals.plugins || (visuals.plugins = {}));
    })(visuals = powerbi.visuals || (powerbi.visuals = {}));
})(powerbi || (powerbi = {}));
